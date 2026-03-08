-- Check if current or historical data exists (use institutional_header as representative)
{% set has_current = check_table_exists('raw', 'institutional_header_current') %}
{% set has_historical = check_table_exists('raw', 'institutional_header_historical') %}

{% set visit_detail_type_mapping = {
    'institutional': 32855,
    'professional': 32873,
    'pharmacy': 32869
} %}

{% set visit_detail_concept_mapping = {
    'institutional': 8717,
    'professional': 8716,
    'pharmacy': 38004338
} %}

{% set cte_queries = [] %}

{% if has_current %}
  -- Institutional detail current
  {% set query %}
  institutional_detail_current as (
    select
      cast(hash(concat_ws('||', d.bill_id, d.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
      {{ derive_person_id('h') }} as person_id,
      {{ visit_detail_concept_mapping['institutional'] }} as visit_detail_concept_id,
      CASE WHEN d.service_line_from_date = 'N' THEN NULL
          ELSE try_cast(d.service_line_from_date as date) END as visit_detail_start_date,
      CASE WHEN d.service_line_from_date = 'N' THEN NULL
          ELSE try_cast(d.service_line_from_date as timestamp) END as visit_detail_start_datetime,
      -- Fallback to visit end date (reporting_period_end_date) when service_line_to_date is null
      COALESCE(
          CASE WHEN d.service_line_to_date = 'N' THEN NULL ELSE try_cast(d.service_line_to_date as date) END,
          try_cast(h.reporting_period_end_date as date)
      ) as visit_detail_end_date,
      COALESCE(
          CASE WHEN d.service_line_to_date = 'N' THEN NULL ELSE try_cast(d.service_line_to_date as timestamp) END,
          try_cast(h.reporting_period_end_date as timestamp)
      ) as visit_detail_end_datetime,
      {{ visit_detail_type_mapping['institutional'] }} as visit_detail_type_concept_id,
      cast(hash(concat_ws('||',
        h.rendering_bill_provider_last,
        coalesce(h.rendering_bill_provider_first, ''),
        h.rendering_bill_provider_state_1,
        h.rendering_bill_provider_4
      ), 'xxhash64') % 1000000000 as varchar) as provider_id,
      cast(
        hash(concat_ws('||',
          h.billing_provider_last_name
        ), 'xxhash64') % 1000000000
      as varchar) as care_site_id,
      cast(d.bill_id as varchar) as visit_occurrence_id,
      cast(null as varchar) as visit_detail_source_value,
      cast(null as integer) as visit_detail_source_concept_id,
      cast(null as integer) as admitted_from_concept_id,
      cast(null as varchar) as admitted_from_source_value,
      cast(null as integer) as discharged_to_concept_id,
      cast(null as varchar) as discharged_to_source_value,
      cast(null as varchar) as preceding_visit_detail_id,
      cast(null as varchar) as parent_visit_detail_id
    from {{ source('raw', 'institutional_detail_current') }} d
    join {{ source('raw', 'institutional_header_current') }} h
      on cast(d.bill_id as varchar) = cast(h.bill_id as varchar)
  )
  {% endset %}
  {% do cte_queries.append(query) %}

  -- Professional detail current
  {% set query %}
  professional_detail_current as (
    select
      cast(hash(concat_ws('||', d.bill_id, d.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
      {{ derive_person_id('h') }} as person_id,
      {{ visit_detail_concept_mapping['professional'] }} as visit_detail_concept_id,
      try_cast(d.service_line_from_date as date) as visit_detail_start_date,
      try_cast(d.service_line_from_date as timestamp) as visit_detail_start_datetime,
      -- Fallback to visit end date (reporting_period_end_date) when service_line_to_date is null
      COALESCE(
          try_cast(d.service_line_to_date as date),
          try_cast(h.reporting_period_end_date as date)
      ) as visit_detail_end_date,
      COALESCE(
          try_cast(d.service_line_to_date as timestamp),
          try_cast(h.reporting_period_end_date as timestamp)
      ) as visit_detail_end_datetime,
      {{ visit_detail_type_mapping['professional'] }} as visit_detail_type_concept_id,
      cast(hash(concat_ws('||',
        h.rendering_bill_provider_last,
        coalesce(h.rendering_bill_provider_first, ''),
        h.rendering_bill_provider_state_1,
        h.rendering_bill_provider_4
      ), 'xxhash64') % 1000000000 as varchar) as provider_id,
      cast(
        hash(concat_ws('||',
          h.billing_provider_last_name,
          h.facility_primary_address
        ), 'xxhash64') % 1000000000
      as varchar) as care_site_id,
      cast(d.bill_id as varchar) as visit_occurrence_id,
      cast(null as varchar) as visit_detail_source_value,
      cast(null as integer) as visit_detail_source_concept_id,
      cast(null as integer) as admitted_from_concept_id,
      cast(null as varchar) as admitted_from_source_value,
      cast(null as integer) as discharged_to_concept_id,
      cast(null as varchar) as discharged_to_source_value,
      cast(null as varchar) as preceding_visit_detail_id,
      cast(null as varchar) as parent_visit_detail_id
    from {{ source('raw', 'professional_detail_current') }} d
    join {{ source('raw', 'professional_header_current') }} h
      on cast(d.bill_id as varchar) = cast(h.bill_id as varchar)
  )
  {% endset %}
  {% do cte_queries.append(query) %}

  -- Pharmacy detail current
  {% set query %}
  pharmacy_detail_current as (
    select
      cast(hash(concat_ws('||', d.bill_id, d.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
      {{ derive_person_id('h') }} as person_id,
      {{ visit_detail_concept_mapping['pharmacy'] }} as visit_detail_concept_id,
      coalesce(try_cast(d.service_line_from_date as date), try_cast(d.prescription_line_date as date)) as visit_detail_start_date,
      coalesce(try_cast(d.service_line_from_date as timestamp), try_cast(d.prescription_line_date as timestamp)) as visit_detail_start_datetime,
      -- Fallback to visit end date (reporting_period_end_date) when service_line_to_date is null
      COALESCE(
          try_cast(d.service_line_to_date as date),
          try_cast(h.reporting_period_end_date as date)
      ) as visit_detail_end_date,
      COALESCE(
          try_cast(d.service_line_to_date as timestamp),
          try_cast(h.reporting_period_end_date as timestamp)
      ) as visit_detail_end_datetime,
      {{ visit_detail_type_mapping['pharmacy'] }} as visit_detail_type_concept_id,
      cast(hash(concat_ws('||',
        h.rendering_bill_provider_last,
        coalesce(h.rendering_bill_provider_first, ''),
        h.rendering_bill_provider_state_1,
        h.rendering_bill_provider_4
      ), 'xxhash64') % 1000000000 as varchar) as provider_id,
      cast(
        hash(concat_ws('||',
          h.billing_provider_last_name,
          h.billing_provider_fein
        ), 'xxhash64') % 1000000000
      as varchar) as care_site_id,
      cast(d.bill_id as varchar) as visit_occurrence_id,
      cast(null as varchar) as visit_detail_source_value,
      cast(null as integer) as visit_detail_source_concept_id,
      cast(null as integer) as admitted_from_concept_id,
      cast(null as varchar) as admitted_from_source_value,
      cast(null as integer) as discharged_to_concept_id,
      cast(null as varchar) as discharged_to_source_value,
      cast(null as varchar) as preceding_visit_detail_id,
      cast(null as varchar) as parent_visit_detail_id
    from {{ source('raw', 'pharmacy_detail_current') }} d
    join {{ source('raw', 'pharmacy_header_current') }} h
      on cast(d.bill_id as varchar) = cast(h.bill_id as varchar)
  )
  {% endset %}
  {% do cte_queries.append(query) %}
{% endif %}

{% if has_historical %}
  -- Institutional detail historical
  {% set query %}
  institutional_detail_historical as (
    select
      cast(hash(concat_ws('||', d.bill_id, d.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
      {{ derive_person_id('h') }} as person_id,
      {{ visit_detail_concept_mapping['institutional'] }} as visit_detail_concept_id,
      CASE WHEN d.service_line_from_date = 'N' THEN NULL
          ELSE try_cast(d.service_line_from_date as date) END as visit_detail_start_date,
      CASE WHEN d.service_line_from_date = 'N' THEN NULL
          ELSE try_cast(d.service_line_from_date as timestamp) END as visit_detail_start_datetime,
      -- Fallback to visit end date (reporting_period_end_date) when service_line_to_date is null
      COALESCE(
          CASE WHEN d.service_line_to_date = 'N' THEN NULL ELSE try_cast(d.service_line_to_date as date) END,
          try_cast(h.reporting_period_end_date as date)
      ) as visit_detail_end_date,
      COALESCE(
          CASE WHEN d.service_line_to_date = 'N' THEN NULL ELSE try_cast(d.service_line_to_date as timestamp) END,
          try_cast(h.reporting_period_end_date as timestamp)
      ) as visit_detail_end_datetime,
      {{ visit_detail_type_mapping['institutional'] }} as visit_detail_type_concept_id,
      cast(hash(concat_ws('||',
        h.rendering_bill_provider_last,
        coalesce(h.rendering_bill_provider_first, ''),
        h.rendering_bill_provider_state_1,
        h.rendering_bill_provider_4
      ), 'xxhash64') % 1000000000 as varchar) as provider_id,
      cast(
        hash(concat_ws('||',
          h.billing_provider_last_name
        ), 'xxhash64') % 1000000000
      as varchar) as care_site_id,
      cast(d.bill_id as varchar) as visit_occurrence_id,
      cast(null as varchar) as visit_detail_source_value,
      cast(null as integer) as visit_detail_source_concept_id,
      cast(null as integer) as admitted_from_concept_id,
      cast(null as varchar) as admitted_from_source_value,
      cast(null as integer) as discharged_to_concept_id,
      cast(null as varchar) as discharged_to_source_value,
      cast(null as varchar) as preceding_visit_detail_id,
      cast(null as varchar) as parent_visit_detail_id
    from {{ source('raw', 'institutional_detail_historical') }} d
    join {{ source('raw', 'institutional_header_historical') }} h
      on cast(d.bill_id as varchar) = cast(h.bill_id as varchar)
  )
  {% endset %}
  {% do cte_queries.append(query) %}

  -- Professional detail historical
  {% set query %}
  professional_detail_historical as (
    select
      cast(hash(concat_ws('||', d.bill_id, d.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
      {{ derive_person_id('h') }} as person_id,
      {{ visit_detail_concept_mapping['professional'] }} as visit_detail_concept_id,
      try_cast(d.service_line_from_date as date) as visit_detail_start_date,
      try_cast(d.service_line_from_date as timestamp) as visit_detail_start_datetime,
      -- Fallback to visit end date (reporting_period_end_date) when service_line_to_date is null
      COALESCE(
          try_cast(d.service_line_to_date as date),
          try_cast(h.reporting_period_end_date as date)
      ) as visit_detail_end_date,
      COALESCE(
          try_cast(d.service_line_to_date as timestamp),
          try_cast(h.reporting_period_end_date as timestamp)
      ) as visit_detail_end_datetime,
      {{ visit_detail_type_mapping['professional'] }} as visit_detail_type_concept_id,
      cast(hash(concat_ws('||',
        h.rendering_bill_provider_last,
        coalesce(h.rendering_bill_provider_first, ''),
        h.rendering_bill_provider_state_1,
        h.rendering_bill_provider_4
      ), 'xxhash64') % 1000000000 as varchar) as provider_id,
      cast(
        hash(concat_ws('||',
          h.billing_provider_last_name,
          h.facility_primary_address
        ), 'xxhash64') % 1000000000
      as varchar) as care_site_id,
      cast(d.bill_id as varchar) as visit_occurrence_id,
      cast(null as varchar) as visit_detail_source_value,
      cast(null as integer) as visit_detail_source_concept_id,
      cast(null as integer) as admitted_from_concept_id,
      cast(null as varchar) as admitted_from_source_value,
      cast(null as integer) as discharged_to_concept_id,
      cast(null as varchar) as discharged_to_source_value,
      cast(null as varchar) as preceding_visit_detail_id,
      cast(null as varchar) as parent_visit_detail_id
    from {{ source('raw', 'professional_detail_historical') }} d
    join {{ source('raw', 'professional_header_historical') }} h
      on cast(d.bill_id as varchar) = cast(h.bill_id as varchar)
  )
  {% endset %}
  {% do cte_queries.append(query) %}

  -- Pharmacy detail historical
  {% set query %}
  pharmacy_detail_historical as (
    select
      cast(hash(concat_ws('||', d.bill_id, d.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
      {{ derive_person_id('h') }} as person_id,
      {{ visit_detail_concept_mapping['pharmacy'] }} as visit_detail_concept_id,
      coalesce(try_cast(d.service_line_from_date as date), try_cast(d.prescription_line_date as date)) as visit_detail_start_date,
      coalesce(try_cast(d.service_line_from_date as timestamp), try_cast(d.prescription_line_date as timestamp)) as visit_detail_start_datetime,
      -- Fallback to visit end date (reporting_period_end_date) when service_line_to_date is null
      COALESCE(
          try_cast(d.service_line_to_date as date),
          try_cast(h.reporting_period_end_date as date)
      ) as visit_detail_end_date,
      COALESCE(
          try_cast(d.service_line_to_date as timestamp),
          try_cast(h.reporting_period_end_date as timestamp)
      ) as visit_detail_end_datetime,
      {{ visit_detail_type_mapping['pharmacy'] }} as visit_detail_type_concept_id,
      cast(hash(concat_ws('||',
        h.rendering_bill_provider_last,
        coalesce(h.rendering_bill_provider_first, ''),
        h.rendering_bill_provider_state_1,
        h.rendering_bill_provider_4
      ), 'xxhash64') % 1000000000 as varchar) as provider_id,
      cast(
        hash(concat_ws('||',
          h.billing_provider_last_name,
          h.billing_provider_fein
        ), 'xxhash64') % 1000000000
      as varchar) as care_site_id,
      cast(d.bill_id as varchar) as visit_occurrence_id,
      cast(null as varchar) as visit_detail_source_value,
      cast(null as integer) as visit_detail_source_concept_id,
      cast(null as integer) as admitted_from_concept_id,
      cast(null as varchar) as admitted_from_source_value,
      cast(null as integer) as discharged_to_concept_id,
      cast(null as varchar) as discharged_to_source_value,
      cast(null as varchar) as preceding_visit_detail_id,
      cast(null as varchar) as parent_visit_detail_id
    from {{ source('raw', 'pharmacy_detail_historical') }} d
    join {{ source('raw', 'pharmacy_header_historical') }} h
      on cast(d.bill_id as varchar) = cast(h.bill_id as varchar)
  )
  {% endset %}
  {% do cte_queries.append(query) %}
{% endif %}

{% set union_queries = [] %}
{% if has_current %}
  {% do union_queries.append('select * from institutional_detail_current') %}
  {% do union_queries.append('select * from professional_detail_current') %}
  {% do union_queries.append('select * from pharmacy_detail_current') %}
{% endif %}
{% if has_historical %}
  {% do union_queries.append('select * from institutional_detail_historical') %}
  {% do union_queries.append('select * from professional_detail_historical') %}
  {% do union_queries.append('select * from pharmacy_detail_historical') %}
{% endif %}

{% if has_current or has_historical %}
with {{ cte_queries | join(",\n") }}

select *
from (
  {{ union_queries | join(" union all ") }}
) as final_result
{% else %}
-- No source tables available - return empty result set with OMOP visit_detail schema
select
    cast(null as varchar) as visit_detail_id,
    cast(null as integer) as person_id,
    cast(null as integer) as visit_detail_concept_id,
    cast(null as date) as visit_detail_start_date,
    cast(null as timestamp) as visit_detail_start_datetime,
    cast(null as date) as visit_detail_end_date,
    cast(null as timestamp) as visit_detail_end_datetime,
    cast(null as integer) as visit_detail_type_concept_id,
    cast(null as varchar) as provider_id,
    cast(null as varchar) as care_site_id,
    cast(null as varchar) as visit_occurrence_id,
    cast(null as varchar) as visit_detail_source_value,
    cast(null as integer) as visit_detail_source_concept_id,
    cast(null as integer) as admitted_from_concept_id,
    cast(null as varchar) as admitted_from_source_value,
    cast(null as integer) as discharged_to_concept_id,
    cast(null as varchar) as discharged_to_source_value,
    cast(null as varchar) as preceding_visit_detail_id,
    cast(null as varchar) as parent_visit_detail_id
where false
{% endif %}
