-- Check if current or historical data exists (use institutional_header as representative)
{% set has_current = check_table_exists('raw', 'institutional_header_current') %}
{% set has_historical = check_table_exists('raw', 'institutional_header_historical') %}

{% set cte_definitions = [] %}
{% set union_queries = [] %}

{% if has_current %}
  {% set query %}
institutional_header_current as (
  select distinct
      cast(bill_id as varchar) as visit_occurrence_id,
      {{ derive_person_id() }} as person_id,
      8717 as visit_concept_id,
      -- Use admission_date when available (more accurate than reporting_period for institutional)
      coalesce(cast(admission_date as date), cast(reporting_period_start_date as date)) as visit_start_date,
      -- Build datetime from admission_date + admission_hour using try_strptime
      case
        when admission_date is not null and admission_hour is not null then
          cast(cast(admission_date as date) as timestamp) +
          extract(hour from coalesce(
            try_strptime(trim(admission_hour), '%I:%M:%S %p'),
            try_strptime(trim(admission_hour), '%I:%M %p')
          ))::integer * interval '1 hour' +
          extract(minute from coalesce(
            try_strptime(trim(admission_hour), '%I:%M:%S %p'),
            try_strptime(trim(admission_hour), '%I:%M %p')
          ))::integer * interval '1 minute'
        when admission_date is not null then cast(admission_date as timestamp)
        else cast(reporting_period_start_date as timestamp)
      end as visit_start_datetime,
      -- Use discharge_date when available
      coalesce(cast(discharge_date as date), cast(reporting_period_end_date as date)) as visit_end_date,
      case
        when discharge_date is not null and discharge_hour is not null then
          cast(cast(discharge_date as date) as timestamp) +
          extract(hour from coalesce(
            try_strptime(trim(discharge_hour), '%I:%M:%S %p'),
            try_strptime(trim(discharge_hour), '%I:%M %p')
          ))::integer * interval '1 hour' +
          extract(minute from coalesce(
            try_strptime(trim(discharge_hour), '%I:%M:%S %p'),
            try_strptime(trim(discharge_hour), '%I:%M %p')
          ))::integer * interval '1 minute'
        when discharge_date is not null then cast(discharge_date as timestamp)
        else cast(reporting_period_end_date as timestamp)
      end as visit_end_datetime,
      32855 as visit_type_concept_id,
      cast(
        hash(concat_ws('||', rendering_bill_provider_last,
          coalesce(rendering_bill_provider_first, ''),
          rendering_bill_provider_state_1,
          rendering_bill_provider_4), 'xxhash64') % 1000000000 as varchar
      ) as provider_id,
      cast(
        hash(concat_ws('||', billing_provider_last_name), 'xxhash64') % 1000000000 as varchar
      ) as care_site_id,
      cast(facility_code as varchar) as visit_source_value,
      cast(null as integer) as visit_source_concept_id,
      case
        when admission_type_code = '1' then 32203
        when admission_type_code = '2' then 32204
        when admission_type_code = '3' then 32205
        when admission_type_code = '4' then 32206
        when admission_type_code = '5' then 32207
        when admission_type_code = '9' then 32208
        else null
      end as admitted_from_concept_id,
      admission_type_code as admitted_from_source_value,
      cast(null as integer) as discharged_to_concept_id,
      cast(null as integer) as discharged_to_source_value,
      cast(null as integer) as preceding_visit_occurrence_id
  from {{ source('raw', 'institutional_header_current') }}
)
  {% endset %}
  {% do cte_definitions.append(query) %}
  {% do union_queries.append('select * from institutional_header_current') %}

  {% set query %}
professional_header_current as (
  select distinct
      cast(bill_id as varchar) as visit_occurrence_id,
      {{ derive_person_id() }} as person_id,
      8716 as visit_concept_id,
      cast(reporting_period_start_date as date) as visit_start_date,
      cast(reporting_period_start_date as timestamp) as visit_start_datetime,
      cast(reporting_period_end_date as date) as visit_end_date,
      cast(reporting_period_end_date as timestamp) as visit_end_datetime,
      32873 as visit_type_concept_id,
      cast(
        hash(concat_ws('||', rendering_bill_provider_last,
          coalesce(rendering_bill_provider_first, ''),
          rendering_bill_provider_state_1,
          rendering_bill_provider_4), 'xxhash64') % 1000000000 as varchar
      ) as provider_id,
      cast(
        hash(concat_ws('||', billing_provider_last_name, facility_primary_address), 'xxhash64') % 1000000000 as varchar
      ) as care_site_id,
      cast(place_of_service_bill_code as varchar) as visit_source_value,
      cast(null as integer) as visit_source_concept_id,
      cast(null as integer) as admitted_from_concept_id,
      cast(null as integer) as admitted_from_source_value,
      cast(null as integer) as discharged_to_concept_id,
      cast(null as integer) as discharged_to_source_value,
      cast(null as integer) as preceding_visit_occurrence_id
  from {{ source('raw', 'professional_header_current') }}
)
  {% endset %}
  {% do cte_definitions.append(query) %}
  {% do union_queries.append('select * from professional_header_current') %}

  {% set query %}
pharmacy_header_current as (
  select distinct
      cast(bill_id as varchar) as visit_occurrence_id,
      {{ derive_person_id() }} as person_id,
      38004338 as visit_concept_id,
      cast(reporting_period_start_date as date) as visit_start_date,
      cast(reporting_period_start_date as timestamp) as visit_start_datetime,
      cast(reporting_period_end_date as date) as visit_end_date,
      cast(reporting_period_end_date as timestamp) as visit_end_datetime,
      32869 as visit_type_concept_id,
      cast(
        hash(concat_ws('||', rendering_bill_provider_last,
          coalesce(rendering_bill_provider_first, ''),
          rendering_bill_provider_state_1,
          rendering_bill_provider_4), 'xxhash64') % 1000000000 as varchar
      ) as provider_id,
      cast(
        hash(concat_ws('||', billing_provider_last_name, billing_provider_fein), 'xxhash64') % 1000000000 as varchar
      ) as care_site_id,
      cast(place_of_service_bill_code as varchar) as visit_source_value,
      cast(null as integer) as visit_source_concept_id,
      cast(null as integer) as admitted_from_concept_id,
      cast(null as integer) as admitted_from_source_value,
      cast(null as integer) as discharged_to_concept_id,
      cast(null as integer) as discharged_to_source_value,
      cast(null as integer) as preceding_visit_occurrence_id
  from {{ source('raw', 'pharmacy_header_current') }}
)
  {% endset %}
  {% do cte_definitions.append(query) %}
  {% do union_queries.append('select * from pharmacy_header_current') %}
{% endif %}

{% if has_historical %}
  {% set query %}
institutional_header_historical as (
  select distinct
      cast(bill_id as varchar) as visit_occurrence_id,
      {{ derive_person_id() }} as person_id,
      8717 as visit_concept_id,
      -- Use admission_date when available (more accurate than reporting_period for institutional)
      coalesce(cast(admission_date as date), cast(reporting_period_start_date as date)) as visit_start_date,
      case
        when admission_date is not null and admission_hour is not null then
          cast(cast(admission_date as date) as timestamp) +
          extract(hour from coalesce(
            try_strptime(trim(admission_hour), '%I:%M:%S %p'),
            try_strptime(trim(admission_hour), '%I:%M %p')
          ))::integer * interval '1 hour' +
          extract(minute from coalesce(
            try_strptime(trim(admission_hour), '%I:%M:%S %p'),
            try_strptime(trim(admission_hour), '%I:%M %p')
          ))::integer * interval '1 minute'
        when admission_date is not null then cast(admission_date as timestamp)
        else cast(reporting_period_start_date as timestamp)
      end as visit_start_datetime,
      coalesce(cast(discharge_date as date), cast(reporting_period_end_date as date)) as visit_end_date,
      case
        when discharge_date is not null and discharge_hour is not null then
          cast(cast(discharge_date as date) as timestamp) +
          extract(hour from coalesce(
            try_strptime(trim(discharge_hour), '%I:%M:%S %p'),
            try_strptime(trim(discharge_hour), '%I:%M %p')
          ))::integer * interval '1 hour' +
          extract(minute from coalesce(
            try_strptime(trim(discharge_hour), '%I:%M:%S %p'),
            try_strptime(trim(discharge_hour), '%I:%M %p')
          ))::integer * interval '1 minute'
        when discharge_date is not null then cast(discharge_date as timestamp)
        else cast(reporting_period_end_date as timestamp)
      end as visit_end_datetime,
      32855 as visit_type_concept_id,
      cast(
        hash(concat_ws('||', rendering_bill_provider_last,
          coalesce(rendering_bill_provider_first, ''),
          rendering_bill_provider_state_1,
          rendering_bill_provider_4), 'xxhash64') % 1000000000 as varchar
      ) as provider_id,
      cast(
        hash(concat_ws('||', billing_provider_last_name), 'xxhash64') % 1000000000 as varchar
      ) as care_site_id,
      cast(facility_code as varchar) as visit_source_value,
      cast(null as integer) as visit_source_concept_id,
      case
        when admission_type_code = '1' then 32203
        when admission_type_code = '2' then 32204
        when admission_type_code = '3' then 32205
        when admission_type_code = '4' then 32206
        when admission_type_code = '5' then 32207
        when admission_type_code = '9' then 32208
        else null
      end as admitted_from_concept_id,
      admission_type_code as admitted_from_source_value,
      cast(null as integer) as discharged_to_concept_id,
      cast(null as integer) as discharged_to_source_value,
      cast(null as integer) as preceding_visit_occurrence_id
  from {{ source('raw', 'institutional_header_historical') }}
)
  {% endset %}
  {% do cte_definitions.append(query) %}
  {% do union_queries.append('select * from institutional_header_historical') %}

  {% set query %}
professional_header_historical as (
  select distinct
      cast(bill_id as varchar) as visit_occurrence_id,
      {{ derive_person_id() }} as person_id,
      8716 as visit_concept_id,
      cast(reporting_period_start_date as date) as visit_start_date,
      cast(reporting_period_start_date as timestamp) as visit_start_datetime,
      cast(reporting_period_end_date as date) as visit_end_date,
      cast(reporting_period_end_date as timestamp) as visit_end_datetime,
      32873 as visit_type_concept_id,
      cast(
        hash(concat_ws('||', rendering_bill_provider_last,
          coalesce(rendering_bill_provider_first, ''),
          rendering_bill_provider_state_1,
          rendering_bill_provider_4), 'xxhash64') % 1000000000 as varchar
      ) as provider_id,
      cast(
        hash(concat_ws('||', billing_provider_last_name, facility_primary_address), 'xxhash64') % 1000000000 as varchar
      ) as care_site_id,
      cast(place_of_service_bill_code as varchar) as visit_source_value,
      cast(null as integer) as visit_source_concept_id,
      cast(null as integer) as admitted_from_concept_id,
      cast(null as integer) as admitted_from_source_value,
      cast(null as integer) as discharged_to_concept_id,
      cast(null as integer) as discharged_to_source_value,
      cast(null as integer) as preceding_visit_occurrence_id
  from {{ source('raw', 'professional_header_historical') }}
)
  {% endset %}
  {% do cte_definitions.append(query) %}
  {% do union_queries.append('select * from professional_header_historical') %}

  {% set query %}
pharmacy_header_historical as (
  select distinct
      cast(bill_id as varchar) as visit_occurrence_id,
      {{ derive_person_id() }} as person_id,
      38004338 as visit_concept_id,
      cast(reporting_period_start_date as date) as visit_start_date,
      cast(reporting_period_start_date as timestamp) as visit_start_datetime,
      cast(reporting_period_end_date as date) as visit_end_date,
      cast(reporting_period_end_date as timestamp) as visit_end_datetime,
      32869 as visit_type_concept_id,
      cast(
        hash(concat_ws('||', rendering_bill_provider_last,
          coalesce(rendering_bill_provider_first, ''),
          rendering_bill_provider_state_1,
          rendering_bill_provider_4), 'xxhash64') % 1000000000 as varchar
      ) as provider_id,
      cast(
        hash(concat_ws('||', billing_provider_last_name, billing_provider_fein), 'xxhash64') % 1000000000 as varchar
      ) as care_site_id,
      cast(place_of_service_bill_code as varchar) as visit_source_value,
      cast(null as integer) as visit_source_concept_id,
      cast(null as integer) as admitted_from_concept_id,
      cast(null as integer) as admitted_from_source_value,
      cast(null as integer) as discharged_to_concept_id,
      cast(null as integer) as discharged_to_source_value,
      cast(null as integer) as preceding_visit_occurrence_id
  from {{ source('raw', 'pharmacy_header_historical') }}
)
  {% endset %}
  {% do cte_definitions.append(query) %}
  {% do union_queries.append('select * from pharmacy_header_historical') %}
{% endif %}

{% if has_current or has_historical %}
with
  {{ cte_definitions | join(",\n") }}

select *
from (
  {{ union_queries | join(" union ") }}
) as final_result
{% else %}
-- No source tables available - return empty result set with OMOP visit_occurrence schema
select
    cast(null as varchar) as visit_occurrence_id,
    cast(null as integer) as person_id,
    cast(null as integer) as visit_concept_id,
    cast(null as date) as visit_start_date,
    cast(null as timestamp) as visit_start_datetime,
    cast(null as date) as visit_end_date,
    cast(null as timestamp) as visit_end_datetime,
    cast(null as integer) as visit_type_concept_id,
    cast(null as varchar) as provider_id,
    cast(null as varchar) as care_site_id,
    cast(null as varchar) as visit_source_value,
    cast(null as integer) as visit_source_concept_id,
    cast(null as integer) as admitted_from_concept_id,
    cast(null as varchar) as admitted_from_source_value,
    cast(null as integer) as discharged_to_concept_id,
    cast(null as integer) as discharged_to_source_value,
    cast(null as integer) as preceding_visit_occurrence_id
where false
{% endif %}
