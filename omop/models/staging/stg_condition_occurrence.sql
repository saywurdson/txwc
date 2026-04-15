-- Check if current or historical data exists (use institutional_header as representative)
{% set has_current = check_table_exists('raw', 'institutional_header_current') %}
{% set has_historical = check_table_exists('raw', 'institutional_header_historical') %}

{% set cte_queries = [] %}

{% if has_current %}
  {% set relation_inst = source('raw', 'institutional_header_current') %}
  {% set base_columns_inst = adapter.get_columns_in_relation(relation_inst) | map(attribute='name') | list %}

  {% set icd_columns = [
      ('first_icd_9cm_or_icd_10cm', 3),
      ('second_icd_9cm_or_icd_10cm', 3),
      ('third_icd_9cm_or_icd_10cm', 3),
      ('fourth_icd_9cm_or_icd_10cm', 3),
      ('fifth_icd_9cm_or_icd_10cm', 3),
      ('principal_diagnosis_code', 1),
      ('admitting_diagnosis_code', 2)
  ] %}
  {% set unpivot_values_inst = [] %}
  {% for col, prio in icd_columns %}
    {% if col in base_columns_inst %}
      {% do unpivot_values_inst.append("(" ~ col ~ ", '" ~ col ~ "', " ~ prio|string ~ ")") %}
    {% else %}
      {% do unpivot_values_inst.append("(null, '" ~ col ~ "', " ~ prio|string ~ ")") %}
    {% endif %}
  {% endfor %}

  {% set cte_query %}
  institutional_header_current as (
    with base as (
      select *
      from {{ source('raw', 'institutional_header_current') }}
    ),
    unpivot_cte as (
      select
        base.bill_id,
        t.icd as condition_source_value,
        t.source_column,
        t.priority,
        row_number() over (
          partition by base.bill_id, t.icd
          order by t.priority
        ) as rn
      from base
      cross join lateral (
        values
        {{ unpivot_values_inst | join(",\n") }}
      ) as t(icd, source_column, priority)
      join {{ source('omop','concept') }} as c
        on c.concept_code = t.icd
      where c.domain_id = 'Condition'
        and c.vocabulary_id in ('ICD10CM','ICD9CM')
    ),
    unique_diag as (
      select bill_id, condition_source_value, source_column, priority
      from unpivot_cte
      where rn = 1
    ),
    -- RECOVERY: Extract ICD codes from billing_provider_last_name when columns are shifted
    recovered_conditions as (
      select
        base.bill_id,
        base.billing_provider_last_name as condition_source_value,
        'billing_provider_last_name_recovered' as source_column,
        99 as priority  -- Lowest priority since recovered
      from base
      join {{ source('omop','concept') }} as c
        on c.concept_code = base.billing_provider_last_name
      where c.domain_id = 'Condition'
        and c.vocabulary_id in ('ICD10CM','ICD9CM')
        and LENGTH(base.billing_provider_state_code) > 2  -- Indicates column shift
        -- Only recover if not already in unique_diag
        and base.bill_id not in (select bill_id from unique_diag where condition_source_value = base.billing_provider_last_name)
    ),
    -- Combine normal diagnoses with recovered ones
    all_diagnoses as (
      select bill_id, condition_source_value, source_column, priority
      from unique_diag
      union all
      select bill_id, condition_source_value, source_column, priority
      from recovered_conditions
    )
    select
      cast(hash(concat_ws('||', base.row_id, base.bill_id, all_diagnoses.source_column), 'xxhash64') % 1000000000 as varchar) as condition_occurrence_id,
      {{ derive_person_id('base') }} as person_id,
      cast(null as integer) as condition_concept_id,
      cast(base.reporting_period_start_date as date) as condition_start_date,
      cast(base.reporting_period_start_date as timestamp) as condition_start_datetime,
      cast(base.reporting_period_end_date as date) as condition_end_date,
      cast(base.reporting_period_end_date as timestamp) as condition_end_datetime,
      32855 as condition_type_concept_id,
      case
        when all_diagnoses.source_column = 'principal_diagnosis_code' then 32902
        when all_diagnoses.source_column = 'admitting_diagnosis_code' then 32890
        when all_diagnoses.source_column = 'billing_provider_last_name_recovered' then 32893  -- Secondary diagnosis
        else 32893
      end as condition_status_concept_id,
      cast(null as varchar) as stop_reason,
      cast(hash(concat_ws('||',
        base.rendering_bill_provider_last,
        coalesce(base.rendering_bill_provider_first, ''),
        base.rendering_bill_provider_state_1,
        base.rendering_bill_provider_4
      ), 'xxhash64') % 1000000000 as varchar) as provider_id,
      cast(base.bill_id as varchar) as visit_occurrence_id,
      -- Header-based diagnoses don't have a corresponding detail line, so no visit_detail_id
      cast(null as varchar) as visit_detail_id,
      all_diagnoses.condition_source_value,
      cast(null as integer) as condition_source_concept_id,
      cast(null as varchar) as condition_status_source_value
    from base
    join all_diagnoses on base.bill_id = all_diagnoses.bill_id
  )
  {% endset %}
  {% do cte_queries.append(cte_query) %}

  {% set relation_prof = source('raw', 'professional_header_current') %}
  {% set base_columns_prof = adapter.get_columns_in_relation(relation_prof) | map(attribute='name') | list %}

  {% set icd_columns_prof = [
      ('first_icd_9cm_or_icd_10cm', None),
      ('second_icd_9cm_or_icd_10cm', None),
      ('third_icd_9cm_or_icd_10cm', None),
      ('fourth_icd_9cm_or_icd_10cm', None),
      ('fifth_icd_9cm_or_icd_10cm', None)
  ] %}
  {% set unpivot_values_prof = [] %}
  {% for col, _ in icd_columns_prof %}
    {% if col in base_columns_prof %}
      {% do unpivot_values_prof.append("(" ~ col ~ ", '" ~ col ~ "')") %}
    {% else %}
      {% do unpivot_values_prof.append("(null, '" ~ col ~ "')") %}
    {% endif %}
  {% endfor %}

  {% set cte_query %}
  professional_header_current as (
    with base as (
      select *
      from {{ source('raw', 'professional_header_current') }}
    ),
    unpivot_cte as (
      select
        base.bill_id,
        t.icd as condition_source_value,
        t.source_column
      from base
      cross join lateral (
        values
        {{ unpivot_values_prof | join(",\n") }}
      ) as t(icd, source_column)
      join {{ source('omop','concept') }} as c
        on c.concept_code = t.icd
      where c.domain_id = 'Condition'
        and c.vocabulary_id in ('ICD10CM','ICD9CM')
    )
    select
      cast(hash(concat_ws('||', base.row_id, base.bill_id, unpivot_cte.source_column), 'xxhash64') % 1000000000 as varchar) as condition_occurrence_id,
      {{ derive_person_id('base') }} as person_id,
      cast(null as integer) as condition_concept_id,
      cast(base.reporting_period_start_date as date) as condition_start_date,
      cast(base.reporting_period_start_date as timestamp) as condition_start_datetime,
      cast(base.reporting_period_end_date as date) as condition_end_date,
      cast(base.reporting_period_end_date as timestamp) as condition_end_datetime,
      32873 as condition_type_concept_id,
      32893 as condition_status_concept_id,
      cast(null as varchar) as stop_reason,
      cast(hash(concat_ws('||',
        base.rendering_bill_provider_last,
        coalesce(base.rendering_bill_provider_first, ''),
        base.rendering_bill_provider_state_1,
        base.rendering_bill_provider_4
      ), 'xxhash64') % 1000000000 as varchar) as provider_id,
      cast(base.bill_id as varchar) as visit_occurrence_id,
      -- Header-based diagnoses don't have a corresponding detail line, so no visit_detail_id
      cast(null as varchar) as visit_detail_id,
      unpivot_cte.condition_source_value,
      cast(null as integer) as condition_source_concept_id,
      cast(null as varchar) as condition_status_source_value
    from base
    join unpivot_cte on base.bill_id = unpivot_cte.bill_id
  )
  {% endset %}
  {% do cte_queries.append(cte_query) %}

  {% set relation_pharm = source('raw', 'pharmacy_header_current') %}
  {% set base_columns_pharm = adapter.get_columns_in_relation(relation_pharm) | map(attribute='name') | list %}

  {% set icd_columns_pharm = [
      ('first_icd_9cm_or_icd_10cm', None),
      ('second_icd_9cm_or_icd_10cm', None),
      ('third_icd_9cm_or_icd_10cm', None),
      ('fourth_icd_9cm_or_icd_10cm', None),
      ('fifth_icd_9cm_or_icd_10cm', None)
  ] %}
  {% set unpivot_values_pharm = [] %}
  {% for col, _ in icd_columns_pharm %}
    {% if col in base_columns_pharm %}
      {% do unpivot_values_pharm.append("(" ~ col ~ ", '" ~ col ~ "')") %}
    {% else %}
      {% do unpivot_values_pharm.append("(null, '" ~ col ~ "')") %}
    {% endif %}
  {% endfor %}

  {% set cte_query %}
  pharmacy_header_current as (
    with base as (
      select *
      from {{ source('raw', 'pharmacy_header_current') }}
    ),
    unpivot_cte as (
      select
        base.bill_id,
        t.icd as condition_source_value,
        t.source_column
      from base
      cross join lateral (
        values
        {{ unpivot_values_pharm | join(",\n") }}
      ) as t(icd, source_column)
      join {{ source('omop','concept') }} as c
        on c.concept_code = t.icd
      where c.domain_id = 'Condition'
        and c.vocabulary_id in ('ICD10CM','ICD9CM')
    )
    select
      cast(hash(concat_ws('||', base.row_id, base.bill_id, unpivot_cte.source_column), 'xxhash64') % 1000000000 as varchar) as condition_occurrence_id,
      {{ derive_person_id('base') }} as person_id,
      cast(null as integer) as condition_concept_id,
      cast(base.reporting_period_start_date as date) as condition_start_date,
      cast(base.reporting_period_start_date as timestamp) as condition_start_datetime,
      cast(base.reporting_period_end_date as date) as condition_end_date,
      cast(base.reporting_period_end_date as timestamp) as condition_end_datetime,
      32873 as condition_type_concept_id,
      32893 as condition_status_concept_id,
      cast(null as varchar) as stop_reason,
      cast(hash(concat_ws('||',
        base.rendering_bill_provider_last,
        coalesce(base.rendering_bill_provider_first, ''),
        base.rendering_bill_provider_state_1,
        base.rendering_bill_provider_4
      ), 'xxhash64') % 1000000000 as varchar) as provider_id,
      cast(base.bill_id as varchar) as visit_occurrence_id,
      -- Header-based diagnoses don't have a corresponding detail line, so no visit_detail_id
      cast(null as varchar) as visit_detail_id,
      unpivot_cte.condition_source_value,
      cast(null as integer) as condition_source_concept_id,
      cast(null as varchar) as condition_status_source_value
    from base
    join unpivot_cte on base.bill_id = unpivot_cte.bill_id
  )
  {% endset %}
  {% do cte_queries.append(cte_query) %}
{% endif %}

{% if has_historical %}
  {% set relation_inst_h = source('raw', 'institutional_header_historical') %}
  {% set base_columns_inst_h = adapter.get_columns_in_relation(relation_inst_h) | map(attribute='name') | list %}

  {% set icd_columns = [
      ('first_icd_9cm_or_icd_10cm', 3),
      ('second_icd_9cm_or_icd_10cm', 3),
      ('third_icd_9cm_or_icd_10cm', 3),
      ('fourth_icd_9cm_or_icd_10cm', 3),
      ('fifth_icd_9cm_or_icd_10cm', 3),
      ('principal_diagnosis_code', 1),
      ('admitting_diagnosis_code', 2)
  ] %}
  {% set unpivot_values_inst_h = [] %}
  {% for col, prio in icd_columns %}
    {% if col in base_columns_inst_h %}
      {% do unpivot_values_inst_h.append("(" ~ col ~ ", '" ~ col ~ "', " ~ prio|string ~ ")") %}
    {% else %}
      {% do unpivot_values_inst_h.append("(null, '" ~ col ~ "', " ~ prio|string ~ ")") %}
    {% endif %}
  {% endfor %}

  {% set cte_query %}
  institutional_header_historical as (
    with base as (
      select *
      from {{ source('raw', 'institutional_header_historical') }}
    ),
    unpivot_cte as (
      select
        base.bill_id,
        t.icd as condition_source_value,
        t.source_column,
        t.priority,
        row_number() over (
          partition by base.bill_id, t.icd
          order by t.priority
        ) as rn
      from base
      cross join lateral (
        values
        {{ unpivot_values_inst_h | join(",\n") }}
      ) as t(icd, source_column, priority)
      join {{ source('omop','concept') }} as c
        on c.concept_code = t.icd
      where c.domain_id = 'Condition'
        and c.vocabulary_id in ('ICD10CM','ICD9CM')
    ),
    unique_diag as (
      select bill_id, condition_source_value, source_column, priority
      from unpivot_cte
      where rn = 1
    ),
    -- RECOVERY: Extract ICD codes from billing_provider_last_name when columns are shifted
    recovered_conditions as (
      select
        base.bill_id,
        base.billing_provider_last_name as condition_source_value,
        'billing_provider_last_name_recovered' as source_column,
        99 as priority  -- Lowest priority since recovered
      from base
      join {{ source('omop','concept') }} as c
        on c.concept_code = base.billing_provider_last_name
      where c.domain_id = 'Condition'
        and c.vocabulary_id in ('ICD10CM','ICD9CM')
        and LENGTH(base.billing_provider_state_code) > 2  -- Indicates column shift
        -- Only recover if not already in unique_diag
        and base.bill_id not in (select bill_id from unique_diag where condition_source_value = base.billing_provider_last_name)
    ),
    -- Combine normal diagnoses with recovered ones
    all_diagnoses as (
      select bill_id, condition_source_value, source_column, priority
      from unique_diag
      union all
      select bill_id, condition_source_value, source_column, priority
      from recovered_conditions
    )
    select
      cast(hash(concat_ws('||', base.row_id, base.bill_id, all_diagnoses.source_column), 'xxhash64') % 1000000000 as varchar) as condition_occurrence_id,
      {{ derive_person_id('base') }} as person_id,
      cast(null as integer) as condition_concept_id,
      cast(base.reporting_period_start_date as date) as condition_start_date,
      cast(base.reporting_period_start_date as timestamp) as condition_start_datetime,
      cast(base.reporting_period_end_date as date) as condition_end_date,
      cast(base.reporting_period_end_date as timestamp) as condition_end_datetime,
      32855 as condition_type_concept_id,
      case
        when all_diagnoses.source_column = 'principal_diagnosis_code' then 32902
        when all_diagnoses.source_column = 'admitting_diagnosis_code' then 32890
        when all_diagnoses.source_column = 'billing_provider_last_name_recovered' then 32893  -- Secondary diagnosis
        else 32893
      end as condition_status_concept_id,
      cast(null as varchar) as stop_reason,
      cast(hash(concat_ws('||',
        base.rendering_bill_provider_last,
        coalesce(base.rendering_bill_provider_first, ''),
        base.rendering_bill_provider_state_1,
        base.rendering_bill_provider_4
      ), 'xxhash64') % 1000000000 as varchar) as provider_id,
      cast(base.bill_id as varchar) as visit_occurrence_id,
      -- Header-based diagnoses don't have a corresponding detail line, so no visit_detail_id
      cast(null as varchar) as visit_detail_id,
      all_diagnoses.condition_source_value,
      cast(null as integer) as condition_source_concept_id,
      cast(null as varchar) as condition_status_source_value
    from base
    join all_diagnoses on base.bill_id = all_diagnoses.bill_id
  )
  {% endset %}
  {% do cte_queries.append(cte_query) %}

  {% set relation_prof_h = source('raw', 'professional_header_historical') %}
  {% set base_columns_prof_h = adapter.get_columns_in_relation(relation_prof_h) | map(attribute='name') | list %}

  {% set icd_columns_prof = [
      ('first_icd_9cm_or_icd_10cm', None),
      ('second_icd_9cm_or_icd_10cm', None),
      ('third_icd_9cm_or_icd_10cm', None),
      ('fourth_icd_9cm_or_icd_10cm', None),
      ('fifth_icd_9cm_or_icd_10cm', None)
  ] %}
  {% set unpivot_values_prof_h = [] %}
  {% for col, _ in icd_columns_prof %}
    {% if col in base_columns_prof_h %}
      {% do unpivot_values_prof_h.append("(" ~ col ~ ", '" ~ col ~ "')") %}
    {% else %}
      {% do unpivot_values_prof_h.append("(null, '" ~ col ~ "')") %}
    {% endif %}
  {% endfor %}

  {% set cte_query %}
  professional_header_historical as (
    with base as (
      select *
      from {{ source('raw', 'professional_header_historical') }}
    ),
    unpivot_cte as (
      select
        base.bill_id,
        t.icd as condition_source_value,
        t.source_column
      from base
      cross join lateral (
        values
        {{ unpivot_values_prof_h | join(",\n") }}
      ) as t(icd, source_column)
      join {{ source('omop','concept') }} as c
        on c.concept_code = t.icd
      where c.domain_id = 'Condition'
        and c.vocabulary_id in ('ICD10CM','ICD9CM')
    )
    select
      cast(hash(concat_ws('||', base.row_id, base.bill_id, unpivot_cte.source_column), 'xxhash64') % 1000000000 as varchar) as condition_occurrence_id,
      {{ derive_person_id('base') }} as person_id,
      cast(null as integer) as condition_concept_id,
      cast(base.reporting_period_start_date as date) as condition_start_date,
      cast(base.reporting_period_start_date as timestamp) as condition_start_datetime,
      cast(base.reporting_period_end_date as date) as condition_end_date,
      cast(base.reporting_period_end_date as timestamp) as condition_end_datetime,
      32873 as condition_type_concept_id,
      32893 as condition_status_concept_id,
      cast(null as varchar) as stop_reason,
      cast(hash(concat_ws('||',
        base.rendering_bill_provider_last,
        coalesce(base.rendering_bill_provider_first, ''),
        base.rendering_bill_provider_state_1,
        base.rendering_bill_provider_4
      ), 'xxhash64') % 1000000000 as varchar) as provider_id,
      cast(base.bill_id as varchar) as visit_occurrence_id,
      -- Header-based diagnoses don't have a corresponding detail line, so no visit_detail_id
      cast(null as varchar) as visit_detail_id,
      unpivot_cte.condition_source_value,
      cast(null as integer) as condition_source_concept_id,
      cast(null as varchar) as condition_status_source_value
    from base
    join unpivot_cte on base.bill_id = unpivot_cte.bill_id
  )
  {% endset %}
  {% do cte_queries.append(cte_query) %}

  {% set relation_pharm_h = source('raw', 'pharmacy_header_historical') %}
  {% set base_columns_pharm_h = adapter.get_columns_in_relation(relation_pharm_h) | map(attribute='name') | list %}

  {% set icd_columns_pharm = [
      ('first_icd_9cm_or_icd_10cm', None),
      ('second_icd_9cm_or_icd_10cm', None),
      ('third_icd_9cm_or_icd_10cm', None),
      ('fourth_icd_9cm_or_icd_10cm', None),
      ('fifth_icd_9cm_or_icd_10cm', None)
  ] %}
  {% set unpivot_values_pharm_h = [] %}
  {% for col, _ in icd_columns_pharm %}
    {% if col in base_columns_pharm_h %}
      {% do unpivot_values_pharm_h.append("(" ~ col ~ ", '" ~ col ~ "')") %}
    {% else %}
      {% do unpivot_values_pharm_h.append("(null, '" ~ col ~ "')") %}
    {% endif %}
  {% endfor %}

  {% set cte_query %}
  pharmacy_header_historical as (
    with base as (
      select *
      from {{ source('raw', 'pharmacy_header_historical') }}
    ),
    unpivot_cte as (
      select
        base.bill_id,
        t.icd as condition_source_value,
        t.source_column
      from base
      cross join lateral (
        values
        {{ unpivot_values_pharm_h | join(",\n") }}
      ) as t(icd, source_column)
      join {{ source('omop','concept') }} as c
        on c.concept_code = t.icd
      where c.domain_id = 'Condition'
        and c.vocabulary_id in ('ICD10CM','ICD9CM')
    )
    select
      cast(hash(concat_ws('||', base.row_id, base.bill_id, unpivot_cte.source_column), 'xxhash64') % 1000000000 as varchar) as condition_occurrence_id,
      {{ derive_person_id('base') }} as person_id,
      cast(null as integer) as condition_concept_id,
      cast(base.reporting_period_start_date as date) as condition_start_date,
      cast(base.reporting_period_start_date as timestamp) as condition_start_datetime,
      cast(base.reporting_period_end_date as date) as condition_end_date,
      cast(base.reporting_period_end_date as timestamp) as condition_end_datetime,
      32873 as condition_type_concept_id,
      32893 as condition_status_concept_id,
      cast(null as varchar) as stop_reason,
      cast(hash(concat_ws('||',
        base.rendering_bill_provider_last,
        coalesce(base.rendering_bill_provider_first, ''),
        base.rendering_bill_provider_state_1,
        base.rendering_bill_provider_4
      ), 'xxhash64') % 1000000000 as varchar) as provider_id,
      cast(base.bill_id as varchar) as visit_occurrence_id,
      -- Header-based diagnoses don't have a corresponding detail line, so no visit_detail_id
      cast(null as varchar) as visit_detail_id,
      unpivot_cte.condition_source_value,
      cast(null as integer) as condition_source_concept_id,
      cast(null as varchar) as condition_status_source_value
    from base
    join unpivot_cte on base.bill_id = unpivot_cte.bill_id
  )
  {% endset %}
  {% do cte_queries.append(cte_query) %}
{% endif %}

{% set union_queries = [] %}
{% if has_current %}
  {% do union_queries.append('select * from institutional_header_current') %}
  {% do union_queries.append('select * from professional_header_current') %}
  {% do union_queries.append('select * from pharmacy_header_current') %}
{% endif %}
{% if has_historical %}
  {% do union_queries.append('select * from institutional_header_historical') %}
  {% do union_queries.append('select * from professional_header_historical') %}
  {% do union_queries.append('select * from pharmacy_header_historical') %}
{% endif %}

{% if has_current or has_historical %}
with {{ cte_queries | join(",\n") }}

select *
from (
  {{ union_queries | join(" union all ") }}
) as final_result
{% else %}
-- No source tables available - return empty result set with OMOP condition_occurrence schema
select
    cast(null as varchar) as condition_occurrence_id,
    cast(null as integer) as person_id,
    cast(null as integer) as condition_concept_id,
    cast(null as date) as condition_start_date,
    cast(null as timestamp) as condition_start_datetime,
    cast(null as date) as condition_end_date,
    cast(null as timestamp) as condition_end_datetime,
    cast(null as integer) as condition_type_concept_id,
    cast(null as integer) as condition_status_concept_id,
    cast(null as varchar) as stop_reason,
    cast(null as varchar) as provider_id,
    cast(null as varchar) as visit_occurrence_id,
    cast(null as varchar) as visit_detail_id,
    cast(null as varchar) as condition_source_value,
    cast(null as integer) as condition_source_concept_id,
    cast(null as varchar) as condition_status_source_value
where false
{% endif %}
