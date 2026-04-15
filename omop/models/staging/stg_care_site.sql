{% set has_current = check_table_exists('raw', 'institutional_header_current') %}
{% set has_historical = check_table_exists('raw', 'institutional_header_historical') %}

{% set cte_queries = [] %}

{% if has_current %}
  {% set query %}
institutional_header_current as (
  select distinct
    {{ derive_care_site_id('institutional') }} as care_site_id,
    -- Fix for shifted columns: use billing_provider_state_code when it contains facility name
    -- Detects shifted records where state_code has facility name instead of 2-char state code
    case
      when LENGTH(billing_provider_state_code) > 2
      then billing_provider_state_code  -- Use facility name from shifted column
      else billing_provider_last_name   -- Use normal column
    end as care_site_name,
    8717 as place_of_service_concept_id,
    {{ derive_facility_location_id() }} as location_id,
    facility_national_provider as care_site_source_value,
    cast(null as varchar) as place_of_service_source_value
  from {{ source('raw', 'institutional_header_current') }}
),
professional_header_current as (
  select distinct
    {{ derive_care_site_id('professional') }} as care_site_id,
    billing_provider_last_name as care_site_name,
    8716 as place_of_service_concept_id,
    {{ derive_facility_location_id() }} as location_id,
    facility_national_provider as care_site_source_value,
    cast(place_of_service_bill_code as varchar) as place_of_service_source_value
  from {{ source('raw', 'professional_header_current') }}
),
pharmacy_header_current as (
  select distinct
    {{ derive_care_site_id('pharmacy') }} as care_site_id,
    coalesce(facility_name, billing_provider_last_name) as care_site_name,
    38004338 as place_of_service_concept_id,
    {{ derive_facility_location_id() }} as location_id,
    facility_national_provider as care_site_source_value,
    cast(place_of_service_bill_code as varchar) as place_of_service_source_value
  from {{ source('raw', 'pharmacy_header_current') }}
)
  {% endset %}
  {% do cte_queries.append(query) %}
{% endif %}

{% if has_historical %}
  {% set query %}
institutional_header_historical as (
  select distinct
    {{ derive_care_site_id('institutional') }} as care_site_id,
    case
      when LENGTH(billing_provider_state_code) > 2
      then billing_provider_state_code
      else billing_provider_last_name
    end as care_site_name,
    8717 as place_of_service_concept_id,
    {{ derive_facility_location_id() }} as location_id,
    facility_national_provider as care_site_source_value,
    cast(null as varchar) as place_of_service_source_value
  from {{ source('raw', 'institutional_header_historical') }}
),
professional_header_historical as (
  select distinct
    {{ derive_care_site_id('professional') }} as care_site_id,
    billing_provider_last_name as care_site_name,
    8716 as place_of_service_concept_id,
    {{ derive_facility_location_id() }} as location_id,
    facility_national_provider as care_site_source_value,
    cast(place_of_service_bill_code as varchar) as place_of_service_source_value
  from {{ source('raw', 'professional_header_historical') }}
),
pharmacy_header_historical as (
  select distinct
    {{ derive_care_site_id('pharmacy') }} as care_site_id,
    coalesce(facility_name, billing_provider_last_name) as care_site_name,
    38004338 as place_of_service_concept_id,
    {{ derive_facility_location_id() }} as location_id,
    facility_national_provider as care_site_source_value,
    cast(place_of_service_bill_code as varchar) as place_of_service_source_value
  from {{ source('raw', 'pharmacy_header_historical') }}
)
  {% endset %}
  {% do cte_queries.append(query) %}
{% endif %}

{% if has_current or has_historical %}
with {{ cte_queries | join(",\n") }}
, raw_union as (
  {% if has_current %}
    select * from institutional_header_current
    union all
    select * from professional_header_current
    union all
    select * from pharmacy_header_current
  {% endif %}
  {% if has_current and has_historical %}
    union all
  {% endif %}
  {% if has_historical %}
    select * from institutional_header_historical
    union all
    select * from professional_header_historical
    union all
    select * from pharmacy_header_historical
  {% endif %}
)
select
    care_site_id,
    min(care_site_name) as care_site_name,
    min(place_of_service_concept_id) as place_of_service_concept_id,
    min(location_id) as location_id,
    min(care_site_source_value) as care_site_source_value,
    min(place_of_service_source_value) as place_of_service_source_value
from raw_union
group by care_site_id
{% else %}
-- No source tables available - return empty result set with OMOP care_site schema
select
    cast(null as varchar) as care_site_id,
    cast(null as varchar) as care_site_name,
    cast(null as integer) as place_of_service_concept_id,
    cast(null as varchar) as location_id,
    cast(null as varchar) as care_site_source_value,
    cast(null as varchar) as place_of_service_source_value
where false
{% endif %}
