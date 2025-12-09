{% set table_list = [
    ('institutional_header_current', 'raw', 'institutional'),
    ('institutional_header_historical', 'raw', 'institutional'),
    ('professional_header_current', 'raw', 'professional'),
    ('professional_header_historical', 'raw', 'professional'),
    ('pharmacy_header_current', 'raw', 'pharmacy'),
    ('pharmacy_header_historical', 'raw', 'pharmacy')
] %}

{% set cte_queries = [] %}

{% for table, schema, header_type in table_list %}
  {% if check_table_exists(schema, table) %}
    {% set relation = source(schema, table) %}
    {% set base_columns = adapter.get_columns_in_relation(relation) | map(attribute='name') | list %}
    
    {% if header_type == 'institutional' %}
      {% set icd_columns = [
          ('first_icd_9cm_or_icd_10cm', 3),
          ('second_icd_9cm_or_icd_10cm', 3),
          ('third_icd_9cm_or_icd_10cm', 3),
          ('fourth_icd_9cm_or_icd_10cm', 3),
          ('fifth_icd_9cm_or_icd_10cm', 3),
          ('principal_diagnosis_code', 1),
          ('admitting_diagnosis_code', 2)
      ] %}
      {% set unpivot_values = [] %}
      {% for col, prio in icd_columns %}
        {% if col in base_columns %}
          {% do unpivot_values.append("(" ~ col ~ ", '" ~ col ~ "', " ~ prio|string ~ ")") %}
        {% else %}
          {% do unpivot_values.append("(null, '" ~ col ~ "', " ~ prio|string ~ ")") %}
        {% endif %}
      {% endfor %}
      
      {% set cte_query %}
      {{ table }} as (
        with base as (
          select *
          from {{ source(schema, table) }}
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
            {{ unpivot_values | join(",\n") }}
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
          cast(hash(concat_ws('||', base.row_id, base.bill_id), 'xxhash64') % 1000000000 as varchar) as condition_occurrence_id,
          case
            when base.patient_account_number is null or trim(base.patient_account_number) = ''
            then lpad(
              cast(
                hash(concat_ws('||',
                  coalesce(base.employee_mailing_city, ''),
                  coalesce(base.employee_mailing_state_code, ''),
                  coalesce(base.employee_mailing_postal_code, ''),
                  coalesce(base.employee_mailing_country, ''),
                  coalesce(cast(base.employee_date_of_birth as varchar), ''),
                  coalesce(base.employee_gender_code, '')
                ), 'xxhash64') % 1000000000 as varchar
              ),
              9,
              '0'
            )
            else base.patient_account_number
          end as person_id,
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
      
    {% elif header_type == 'professional' %}
      {% set icd_columns_prof = [
          ('first_icd_9cm_or_icd_10cm', None),
          ('second_icd_9cm_or_icd_10cm', None),
          ('third_icd_9cm_or_icd_10cm', None),
          ('fourth_icd_9cm_or_icd_10cm', None),
          ('fifth_icd_9cm_or_icd_10cm', None)
      ] %}
      {% set unpivot_values_prof = [] %}
      {% for col, _ in icd_columns_prof %}
        {% if col in base_columns %}
          {% do unpivot_values_prof.append("(" ~ col ~ ", '" ~ col ~ "')") %}
        {% else %}
          {% do unpivot_values_prof.append("(null, '" ~ col ~ "')") %}
        {% endif %}
      {% endfor %}
      
      {% set cte_query %}
      {{ table }} as (
        with base as (
          select *
          from {{ source(schema, table) }}
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
          cast(hash(concat_ws('||', base.row_id, base.bill_id), 'xxhash64') % 1000000000 as varchar) as condition_occurrence_id,
          case
            when base.patient_account_number is null or trim(base.patient_account_number) = ''
            then lpad(
              cast(
                hash(concat_ws('||',
                  coalesce(base.employee_mailing_city, ''),
                  coalesce(base.employee_mailing_state_code, ''),
                  coalesce(base.employee_mailing_postal_code, ''),
                  coalesce(base.employee_mailing_country, ''),
                  coalesce(cast(base.employee_date_of_birth as varchar), ''),
                  coalesce(base.employee_gender_code, '')
                ), 'xxhash64') % 1000000000 as varchar
              ),
              9,
              '0'
            )
            else base.patient_account_number
          end as person_id,
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

    {% elif header_type == 'pharmacy' %}
      {% set icd_columns_pharm = [
          ('first_icd_9cm_or_icd_10cm', None),
          ('second_icd_9cm_or_icd_10cm', None),
          ('third_icd_9cm_or_icd_10cm', None),
          ('fourth_icd_9cm_or_icd_10cm', None),
          ('fifth_icd_9cm_or_icd_10cm', None)
      ] %}
      {% set unpivot_values_pharm = [] %}
      {% for col, _ in icd_columns_pharm %}
        {% if col in base_columns %}
          {% do unpivot_values_pharm.append("(" ~ col ~ ", '" ~ col ~ "')") %}
        {% else %}
          {% do unpivot_values_pharm.append("(null, '" ~ col ~ "')") %}
        {% endif %}
      {% endfor %}
      
      {% set cte_query %}
      {{ table }} as (
        with base as (
          select *
          from {{ source(schema, table) }}
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
          cast(hash(concat_ws('||', base.row_id, base.bill_id), 'xxhash64') % 1000000000 as varchar) as condition_occurrence_id,
          case
            when base.patient_account_number is null or trim(base.patient_account_number) = ''
            then lpad(
              cast(
                hash(concat_ws('||',
                  coalesce(base.employee_mailing_city, ''),
                  coalesce(base.employee_mailing_state_code, ''),
                  coalesce(base.employee_mailing_postal_code, ''),
                  coalesce(base.employee_mailing_country, ''),
                  coalesce(cast(base.employee_date_of_birth as varchar), ''),
                  coalesce(base.employee_gender_code, '')
                ), 'xxhash64') % 1000000000 as varchar
              ),
              9,
              '0'
            )
            else base.patient_account_number
          end as person_id,
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

    {% endif %}
    {% do cte_queries.append(cte_query) %}
  {% endif %}
{% endfor %}

{% if cte_queries | length > 0 %}
with {{ cte_queries | join(",\n") }}
{% endif %}

{% set valid_tables = [] %}
{% for table, schema, header_type in table_list %}
  {% if check_table_exists(schema, table) %}
    {% do valid_tables.append(table) %}
  {% endif %}
{% endfor %}

{% if valid_tables | length > 0 %}
select *
from (
  {% for table in valid_tables %}
    select * from {{ table }}
    {% if not loop.last %}
      union all
    {% endif %}
  {% endfor %}
) as final_result
{% else %}
select null as message
{% endif %}