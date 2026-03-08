-- Check if current or historical data exists (use institutional_header as representative)
{% set has_current = check_table_exists('raw', 'institutional_header_current') %}
{% set has_historical = check_table_exists('raw', 'institutional_header_historical') %}

{% set header_ctes = [] %}
{% set detail_ctes = [] %}
{% set union_list = [] %}

{% if has_current %}
  -- Institutional header current (ICD procedure codes)
  {% set icd_columns = [
      ('first_icd_9cm_or_icd_10cm_1', 'first_icd_9cm_or_icd_10cm'),
      ('second_icd_9cm_or_icd_10cm_1', 'second_icd_9cm_or_icd_10cm'),
      ('third_icd_9cm_or_icd_10cm_1', 'third_icd_9cm_or_icd_10cm'),
      ('fourth_icd_9cm_or_icd_10cm_1', 'fourth_icd_9cm_or_icd_10cm'),
      ('icd_9cm_or_icd_10cm_principal', 'icd_9cm_or_icd_10cm_principal')
  ] %}
  {% set unpivot_values = [] %}
  {% for col_source, col_alias in icd_columns %}
    {% do unpivot_values.append("(" ~ col_source ~ ", '" ~ col_alias ~ "')") %}
  {% endfor %}

  {% set query %}
final_ihc as (
  with unpivot_ihc_diagnoses as (
    select
      ihc.bill_id,
      t.icd as procedure_source_value,
      t.source_column
    from {{ source('raw', 'institutional_header_current') }} as ihc
    cross join lateral (
      values
      {{ unpivot_values | join(",\n") }}
    ) as t(icd, source_column)
    join {{ source('omop','concept') }} as c
      on c.concept_code = t.icd
    where c.domain_id = 'Procedure'
      and c.vocabulary_id in ('ICD10PCS','ICD9Proc')
  ),
  -- RECOVERY: Extract ICD procedure codes from billing_provider_last_name when columns are shifted
  recovered_procedures as (
    select
      ihc.bill_id,
      ihc.billing_provider_last_name as procedure_source_value,
      'billing_provider_last_name_recovered' as source_column
    from {{ source('raw', 'institutional_header_current') }} as ihc
    join {{ source('omop','concept') }} as c
      on c.concept_code = ihc.billing_provider_last_name
    where c.domain_id = 'Procedure'
      and c.vocabulary_id in ('ICD10PCS','ICD9Proc')
      and LENGTH(ihc.billing_provider_state_code) > 2  -- Indicates column shift
      -- Only recover if not already in unpivot_ihc_diagnoses
      and ihc.bill_id not in (select bill_id from unpivot_ihc_diagnoses where procedure_source_value = ihc.billing_provider_last_name)
  ),
  -- Combine normal procedures with recovered ones
  all_procedures as (
    select bill_id, procedure_source_value, source_column
    from unpivot_ihc_diagnoses
    union all
    select bill_id, procedure_source_value, source_column
    from recovered_procedures
  )
  select
    cast(hash(concat_ws('||', ihc.bill_id, ihc.row_id), 'xxhash64') % 1000000000 as varchar) as procedure_occurrence_id,
    {{ derive_person_id('ihc') }} as person_id,
    cast(null as integer) as procedure_concept_id,
    case
      when all_procedures.source_column = 'icd_9cm_or_icd_10cm_principal' then cast(ihc.principal_procedure_date as date)
      when all_procedures.source_column = 'first_icd_9cm_or_icd_10cm' then cast(ihc.first_procedure_date as date)
      when all_procedures.source_column = 'second_icd_9cm_or_icd_10cm' then cast(ihc.second_procedure_date as date)
      when all_procedures.source_column = 'third_icd_9cm_or_icd_10cm' then cast(ihc.third_procedure_date as date)
      when all_procedures.source_column = 'fourth_icd_9cm_or_icd_10cm' then cast(ihc.fourth_procedure_date as date)
      when all_procedures.source_column = 'billing_provider_last_name_recovered' then cast(ihc.reporting_period_start_date as date)
      else cast(ihc.principal_procedure_date as date)
    end as procedure_date,
    case
      when all_procedures.source_column = 'icd_9cm_or_icd_10cm_principal' then cast(ihc.principal_procedure_date as timestamp)
      when all_procedures.source_column = 'first_icd_9cm_or_icd_10cm' then cast(ihc.first_procedure_date as timestamp)
      when all_procedures.source_column = 'second_icd_9cm_or_icd_10cm' then cast(ihc.second_procedure_date as timestamp)
      when all_procedures.source_column = 'third_icd_9cm_or_icd_10cm' then cast(ihc.third_procedure_date as timestamp)
      when all_procedures.source_column = 'fourth_icd_9cm_or_icd_10cm' then cast(ihc.fourth_procedure_date as timestamp)
      when all_procedures.source_column = 'billing_provider_last_name_recovered' then cast(ihc.reporting_period_start_date as timestamp)
      else cast(ihc.principal_procedure_date as timestamp)
    end as procedure_datetime,
    -- Fallback to visit end date (reporting_period_end_date) when procedure_end_date is null
    cast(ihc.reporting_period_end_date as date) as procedure_end_date,
    cast(ihc.reporting_period_end_date as timestamp) as procedure_end_datetime,
    32855 as procedure_type_concept_id,
    cast(null as integer) as modifier_concept_id,
    1 as quantity,
    cast(hash(concat_ws('||',
      ihc.rendering_bill_provider_last,
      coalesce(ihc.rendering_bill_provider_first, ''),
      ihc.rendering_bill_provider_state_1,
      ihc.rendering_bill_provider_4
    ), 'xxhash64') % 1000000000 as varchar) as provider_id,
    cast(ihc.bill_id as varchar) as visit_occurrence_id,
    -- Header-based ICD procedures don't have a corresponding detail line, so no visit_detail_id
    cast(null as varchar) as visit_detail_id,
    all_procedures.procedure_source_value,
    cast(null as integer) as procedure_source_concept_id,
    cast(null as varchar) as modifier_source_value
  from {{ source('raw', 'institutional_header_current') }} as ihc
  join all_procedures on cast(ihc.bill_id as varchar) = cast(all_procedures.bill_id as varchar)
)
  {% endset %}
  {% do header_ctes.append(query) %}
  {% do union_list.append("select * from final_ihc") %}

  -- Institutional detail current (HCPCS procedure codes)
  {% set query %}
final_id as (
  select
    cast(hash(concat_ws('||', id.bill_id, id.row_id), 'xxhash64') % 1000000000 as varchar) as procedure_occurrence_id,
    {{ derive_person_id('ihc') }} as person_id,
    cast(null as integer) as procedure_concept_id,
    CASE WHEN id.service_line_from_date = 'N' THEN NULL
        ELSE cast(id.service_line_from_date as date) END as procedure_date,
    CASE WHEN id.service_line_from_date = 'N' THEN NULL
        ELSE cast(id.service_line_from_date as timestamp) END as procedure_datetime,
    -- Fallback to visit end date (reporting_period_end_date) when service_line_to_date is null
    COALESCE(
        CASE WHEN id.service_line_to_date = 'N' THEN NULL ELSE cast(id.service_line_to_date as date) END,
        cast(ihc.reporting_period_end_date as date)
    ) as procedure_end_date,
    COALESCE(
        CASE WHEN id.service_line_to_date = 'N' THEN NULL ELSE cast(id.service_line_to_date as timestamp) END,
        cast(ihc.reporting_period_end_date as timestamp)
    ) as procedure_end_datetime,
    32854 as procedure_type_concept_id,
    cast(null as integer) as modifier_concept_id,
    cast(id.days_units_billed as integer) as quantity,
    cast(hash(concat_ws('||',
      ihc.rendering_bill_provider_last,
      coalesce(ihc.rendering_bill_provider_first, ''),
      ihc.rendering_bill_provider_state_1,
      ihc.rendering_bill_provider_4
    ), 'xxhash64') % 1000000000 as varchar) as provider_id,
    cast(id.bill_id as varchar) as visit_occurrence_id,
    -- Detail-based HCPCS procedures link to visit_detail via bill_id + row_id hash
    cast(hash(concat_ws('||', id.bill_id, id.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
    id.hcpcs_line_procedure_billed as procedure_source_value,
    cast(null as integer) as procedure_source_concept_id,
    concat_ws('|', id.first_hcpcs_modifier_billed, id.second_hcpcs_modifier_billed, id.third_hcpcs_modifier_billed) as modifier_source_value
  from {{ source('raw', 'institutional_detail_current') }} id
  join {{ source('raw', 'institutional_header_current') }} ihc
    on cast(id.bill_id as varchar) = cast(ihc.bill_id as varchar)
  join {{ source('omop','concept') }} as c
    on c.concept_code = id.hcpcs_line_procedure_billed
  where c.domain_id = 'Procedure'
    and c.vocabulary_id in ('CPT4','HCPCS')
)
  {% endset %}
  {% do detail_ctes.append(query) %}
  {% do union_list.append("select * from final_id") %}

  -- Professional detail current (HCPCS procedure codes)
  {% set query %}
final_pdc as (
  select
    cast(hash(concat_ws('||', prd.bill_id, prd.row_id), 'xxhash64') % 1000000000 as varchar) as procedure_occurrence_id,
    {{ derive_person_id('prhc') }} as person_id,
    cast(null as integer) as procedure_concept_id,
    cast(prd.service_line_from_date as date) as procedure_date,
    cast(prd.service_line_from_date as timestamp) as procedure_datetime,
    -- Fallback to visit end date (reporting_period_end_date) when service_line_to_date is null
    COALESCE(
        cast(prd.service_line_to_date as date),
        cast(prhc.reporting_period_end_date as date)
    ) as procedure_end_date,
    COALESCE(
        cast(prd.service_line_to_date as timestamp),
        cast(prhc.reporting_period_end_date as timestamp)
    ) as procedure_end_datetime,
    32854 as procedure_type_concept_id,
    cast(null as integer) as modifier_concept_id,
    cast(prd.days_units_billed as integer) as quantity,
    cast(hash(concat_ws('||',
      prhc.rendering_bill_provider_last,
      coalesce(prhc.rendering_bill_provider_first, ''),
      prhc.rendering_bill_provider_state_1,
      prhc.rendering_bill_provider_4
    ), 'xxhash64') % 1000000000 as varchar) as provider_id,
    cast(prd.bill_id as varchar) as visit_occurrence_id,
    -- Detail-based HCPCS procedures link to visit_detail via bill_id + row_id hash
    cast(hash(concat_ws('||', prd.bill_id, prd.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
    prd.hcpcs_line_procedure_billed as procedure_source_value,
    cast(null as integer) as procedure_source_concept_id,
    concat_ws('|', prd.first_hcpcs_modifier_billed, prd.second_hcpcs_modifier_billed, prd.third_hcpcs_modifier_billed, prd.fourth_hcpcs_modifier_billed) as modifier_source_value
  from {{ source('raw', 'professional_detail_current') }} prd
  join {{ source('raw', 'professional_header_current') }} prhc
    on cast(prd.bill_id as varchar) = cast(prhc.bill_id as varchar)
  join {{ source('omop','concept') }} as c
    on c.concept_code = prd.hcpcs_line_procedure_billed
  where c.domain_id = 'Procedure'
    and c.vocabulary_id in ('CPT4','HCPCS')
)
  {% endset %}
  {% do detail_ctes.append(query) %}
  {% do union_list.append("select * from final_pdc") %}
{% endif %}

{% if has_historical %}
  -- Institutional header historical (ICD procedure codes)
  {% set icd_columns = [
      ('first_icd_9cm_or_icd_10cm_1', 'first_icd_9cm_or_icd_10cm'),
      ('second_icd_9cm_or_icd_10cm_1', 'second_icd_9cm_or_icd_10cm'),
      ('third_icd_9cm_or_icd_10cm_1', 'third_icd_9cm_or_icd_10cm'),
      ('fourth_icd_9cm_or_icd_10cm_1', 'fourth_icd_9cm_or_icd_10cm'),
      ('icd_9cm_or_icd_10cm_principal', 'icd_9cm_or_icd_10cm_principal')
  ] %}
  {% set unpivot_values_h = [] %}
  {% for col_source, col_alias in icd_columns %}
    {% do unpivot_values_h.append("(" ~ col_source ~ ", '" ~ col_alias ~ "')") %}
  {% endfor %}

  {% set query %}
final_ihh as (
  with unpivot_ihc_diagnoses as (
    select
      ihc.bill_id,
      t.icd as procedure_source_value,
      t.source_column
    from {{ source('raw', 'institutional_header_historical') }} as ihc
    cross join lateral (
      values
      {{ unpivot_values_h | join(",\n") }}
    ) as t(icd, source_column)
    join {{ source('omop','concept') }} as c
      on c.concept_code = t.icd
    where c.domain_id = 'Procedure'
      and c.vocabulary_id in ('ICD10PCS','ICD9Proc')
  ),
  -- RECOVERY: Extract ICD procedure codes from billing_provider_last_name when columns are shifted
  recovered_procedures as (
    select
      ihc.bill_id,
      ihc.billing_provider_last_name as procedure_source_value,
      'billing_provider_last_name_recovered' as source_column
    from {{ source('raw', 'institutional_header_historical') }} as ihc
    join {{ source('omop','concept') }} as c
      on c.concept_code = ihc.billing_provider_last_name
    where c.domain_id = 'Procedure'
      and c.vocabulary_id in ('ICD10PCS','ICD9Proc')
      and LENGTH(ihc.billing_provider_state_code) > 2  -- Indicates column shift
      -- Only recover if not already in unpivot_ihc_diagnoses
      and ihc.bill_id not in (select bill_id from unpivot_ihc_diagnoses where procedure_source_value = ihc.billing_provider_last_name)
  ),
  -- Combine normal procedures with recovered ones
  all_procedures as (
    select bill_id, procedure_source_value, source_column
    from unpivot_ihc_diagnoses
    union all
    select bill_id, procedure_source_value, source_column
    from recovered_procedures
  )
  select
    cast(hash(concat_ws('||', ihc.bill_id, ihc.row_id), 'xxhash64') % 1000000000 as varchar) as procedure_occurrence_id,
    {{ derive_person_id('ihc') }} as person_id,
    cast(null as integer) as procedure_concept_id,
    case
      when all_procedures.source_column = 'icd_9cm_or_icd_10cm_principal' then cast(ihc.principal_procedure_date as date)
      when all_procedures.source_column = 'first_icd_9cm_or_icd_10cm' then cast(ihc.first_procedure_date as date)
      when all_procedures.source_column = 'second_icd_9cm_or_icd_10cm' then cast(ihc.second_procedure_date as date)
      when all_procedures.source_column = 'third_icd_9cm_or_icd_10cm' then cast(ihc.third_procedure_date as date)
      when all_procedures.source_column = 'fourth_icd_9cm_or_icd_10cm' then cast(ihc.fourth_procedure_date as date)
      when all_procedures.source_column = 'billing_provider_last_name_recovered' then cast(ihc.reporting_period_start_date as date)
      else cast(ihc.principal_procedure_date as date)
    end as procedure_date,
    case
      when all_procedures.source_column = 'icd_9cm_or_icd_10cm_principal' then cast(ihc.principal_procedure_date as timestamp)
      when all_procedures.source_column = 'first_icd_9cm_or_icd_10cm' then cast(ihc.first_procedure_date as timestamp)
      when all_procedures.source_column = 'second_icd_9cm_or_icd_10cm' then cast(ihc.second_procedure_date as timestamp)
      when all_procedures.source_column = 'third_icd_9cm_or_icd_10cm' then cast(ihc.third_procedure_date as timestamp)
      when all_procedures.source_column = 'fourth_icd_9cm_or_icd_10cm' then cast(ihc.fourth_procedure_date as timestamp)
      when all_procedures.source_column = 'billing_provider_last_name_recovered' then cast(ihc.reporting_period_start_date as timestamp)
      else cast(ihc.principal_procedure_date as timestamp)
    end as procedure_datetime,
    -- Fallback to visit end date (reporting_period_end_date) when procedure_end_date is null
    cast(ihc.reporting_period_end_date as date) as procedure_end_date,
    cast(ihc.reporting_period_end_date as timestamp) as procedure_end_datetime,
    32855 as procedure_type_concept_id,
    cast(null as integer) as modifier_concept_id,
    1 as quantity,
    cast(hash(concat_ws('||',
      ihc.rendering_bill_provider_last,
      coalesce(ihc.rendering_bill_provider_first, ''),
      ihc.rendering_bill_provider_state_1,
      ihc.rendering_bill_provider_4
    ), 'xxhash64') % 1000000000 as varchar) as provider_id,
    cast(ihc.bill_id as varchar) as visit_occurrence_id,
    -- Header-based ICD procedures don't have a corresponding detail line, so no visit_detail_id
    cast(null as varchar) as visit_detail_id,
    all_procedures.procedure_source_value,
    cast(null as integer) as procedure_source_concept_id,
    cast(null as varchar) as modifier_source_value
  from {{ source('raw', 'institutional_header_historical') }} as ihc
  join all_procedures on cast(ihc.bill_id as varchar) = cast(all_procedures.bill_id as varchar)
)
  {% endset %}
  {% do header_ctes.append(query) %}
  {% do union_list.append("select * from final_ihh") %}

  -- Institutional detail historical (HCPCS procedure codes)
  {% set query %}
final_idh as (
  select
    cast(hash(concat_ws('||', id.bill_id, id.row_id), 'xxhash64') % 1000000000 as varchar) as procedure_occurrence_id,
    {{ derive_person_id('ihc') }} as person_id,
    cast(null as integer) as procedure_concept_id,
    CASE WHEN id.service_line_from_date = 'N' THEN NULL
        ELSE cast(id.service_line_from_date as date) END as procedure_date,
    CASE WHEN id.service_line_from_date = 'N' THEN NULL
        ELSE cast(id.service_line_from_date as timestamp) END as procedure_datetime,
    -- Fallback to visit end date (reporting_period_end_date) when service_line_to_date is null
    COALESCE(
        CASE WHEN id.service_line_to_date = 'N' THEN NULL ELSE cast(id.service_line_to_date as date) END,
        cast(ihc.reporting_period_end_date as date)
    ) as procedure_end_date,
    COALESCE(
        CASE WHEN id.service_line_to_date = 'N' THEN NULL ELSE cast(id.service_line_to_date as timestamp) END,
        cast(ihc.reporting_period_end_date as timestamp)
    ) as procedure_end_datetime,
    32854 as procedure_type_concept_id,
    cast(null as integer) as modifier_concept_id,
    cast(id.days_units_billed as integer) as quantity,
    cast(hash(concat_ws('||',
      ihc.rendering_bill_provider_last,
      coalesce(ihc.rendering_bill_provider_first, ''),
      ihc.rendering_bill_provider_state_1,
      ihc.rendering_bill_provider_4
    ), 'xxhash64') % 1000000000 as varchar) as provider_id,
    cast(id.bill_id as varchar) as visit_occurrence_id,
    -- Detail-based HCPCS procedures link to visit_detail via bill_id + row_id hash
    cast(hash(concat_ws('||', id.bill_id, id.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
    id.hcpcs_line_procedure_billed as procedure_source_value,
    cast(null as integer) as procedure_source_concept_id,
    concat_ws('|', id.first_hcpcs_modifier_billed, id.second_hcpcs_modifier_billed, id.third_hcpcs_modifier_billed) as modifier_source_value
  from {{ source('raw', 'institutional_detail_historical') }} id
  join {{ source('raw', 'institutional_header_historical') }} ihc
    on cast(id.bill_id as varchar) = cast(ihc.bill_id as varchar)
  join {{ source('omop','concept') }} as c
    on c.concept_code = id.hcpcs_line_procedure_billed
  where c.domain_id = 'Procedure'
    and c.vocabulary_id in ('CPT4','HCPCS')
)
  {% endset %}
  {% do detail_ctes.append(query) %}
  {% do union_list.append("select * from final_idh") %}

  -- Professional detail historical (HCPCS procedure codes)
  {% set query %}
final_pdh as (
  select
    cast(hash(concat_ws('||', prd.bill_id, prd.row_id), 'xxhash64') % 1000000000 as varchar) as procedure_occurrence_id,
    {{ derive_person_id('prhc') }} as person_id,
    cast(null as integer) as procedure_concept_id,
    cast(prd.service_line_from_date as date) as procedure_date,
    cast(prd.service_line_from_date as timestamp) as procedure_datetime,
    -- Fallback to visit end date (reporting_period_end_date) when service_line_to_date is null
    COALESCE(
        cast(prd.service_line_to_date as date),
        cast(prhc.reporting_period_end_date as date)
    ) as procedure_end_date,
    COALESCE(
        cast(prd.service_line_to_date as timestamp),
        cast(prhc.reporting_period_end_date as timestamp)
    ) as procedure_end_datetime,
    32854 as procedure_type_concept_id,
    cast(null as integer) as modifier_concept_id,
    cast(prd.days_units_billed as integer) as quantity,
    cast(hash(concat_ws('||',
      prhc.rendering_bill_provider_last,
      coalesce(prhc.rendering_bill_provider_first, ''),
      prhc.rendering_bill_provider_state_1,
      prhc.rendering_bill_provider_4
    ), 'xxhash64') % 1000000000 as varchar) as provider_id,
    cast(prd.bill_id as varchar) as visit_occurrence_id,
    -- Detail-based HCPCS procedures link to visit_detail via bill_id + row_id hash
    cast(hash(concat_ws('||', prd.bill_id, prd.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
    prd.hcpcs_line_procedure_billed as procedure_source_value,
    cast(null as integer) as procedure_source_concept_id,
    concat_ws('|', prd.first_hcpcs_modifier_billed, prd.second_hcpcs_modifier_billed, prd.third_hcpcs_modifier_billed, prd.fourth_hcpcs_modifier_billed) as modifier_source_value
  from {{ source('raw', 'professional_detail_historical') }} prd
  join {{ source('raw', 'professional_header_historical') }} prhc
    on cast(prd.bill_id as varchar) = cast(prhc.bill_id as varchar)
  join {{ source('omop','concept') }} as c
    on c.concept_code = prd.hcpcs_line_procedure_billed
  where c.domain_id = 'Procedure'
    and c.vocabulary_id in ('CPT4','HCPCS')
)
  {% endset %}
  {% do detail_ctes.append(query) %}
  {% do union_list.append("select * from final_pdh") %}
{% endif %}

{% if has_current or has_historical %}
with
  {{ (header_ctes | join(",\n")) ~ ((detail_ctes | length > 0) and ",\n" or "") ~ (detail_ctes | join(",\n")) }}

select *
from (
  {{ union_list | join(" union ") }}
) as final_result
{% else %}
-- No source tables available - return empty result set with OMOP procedure_occurrence schema
select
    cast(null as varchar) as procedure_occurrence_id,
    cast(null as integer) as person_id,
    cast(null as integer) as procedure_concept_id,
    cast(null as date) as procedure_date,
    cast(null as timestamp) as procedure_datetime,
    cast(null as date) as procedure_end_date,
    cast(null as timestamp) as procedure_end_datetime,
    cast(null as integer) as procedure_type_concept_id,
    cast(null as integer) as modifier_concept_id,
    cast(null as integer) as quantity,
    cast(null as varchar) as provider_id,
    cast(null as varchar) as visit_occurrence_id,
    cast(null as varchar) as visit_detail_id,
    cast(null as varchar) as procedure_source_value,
    cast(null as integer) as procedure_source_concept_id,
    cast(null as varchar) as modifier_source_value
where false
{% endif %}
