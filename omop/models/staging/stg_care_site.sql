{% set has_current = check_table_exists('raw', 'institutional_header_current') %}
{% set has_historical = check_table_exists('raw', 'institutional_header_historical') %}

{% set cte_queries = [] %}

{% if has_current %}
  {% set query %}
institutional_header_current as (
  select distinct
    -- Include full location info in care_site_id hash to ensure uniqueness
    cast(
      hash(
        concat_ws(
          '||',
          billing_provider_last_name,
          facility_primary_address,
          facility_city,
          facility_state_code,
          facility_postal_code,
          facility_country_code
        )
      , 'xxhash64'
      ) % 1000000000
    as varchar) as care_site_id,
    -- Fix for shifted columns: use billing_provider_state_code when it contains facility name
    -- Detects shifted records where state_code has facility name instead of 2-char state code
    case
      when LENGTH(billing_provider_state_code) > 2
      then billing_provider_state_code  -- Use facility name from shifted column
      else billing_provider_last_name   -- Use normal column
    end as care_site_name,
    8717 as place_of_service_concept_id,
    cast(
      hash(
        concat_ws(
          '||',
          billing_provider_last_name,
          facility_primary_address,
          facility_city,
          facility_state_code,
          facility_postal_code,
          facility_country_code
        )
      , 'xxhash64'
      ) % 1000000000
    as varchar) as location_id,
    cast(null as varchar) as care_site_source_value,
    cast(null as varchar) as place_of_service_source_value
  from {{ source('raw', 'institutional_header_current') }}
),
professional_header_current as (
  select distinct
    cast(
      hash(
        concat_ws(
          '||',
          billing_provider_last_name,
          facility_primary_address,
          facility_city,
          facility_state_code,
          facility_postal_code,
          facility_country_code
        )
      , 'xxhash64'
      ) % 1000000000
    as varchar) as care_site_id,
    billing_provider_last_name as care_site_name,
    8716 as place_of_service_concept_id,
    cast(
      hash(
        concat_ws(
          '||',
          billing_provider_last_name,
          facility_primary_address,
          facility_city,
          facility_state_code,
          facility_postal_code,
          facility_country_code
        )
      , 'xxhash64'
      ) % 1000000000
    as varchar) as location_id,
    cast(null as varchar) as care_site_source_value,
    cast(place_of_service_bill_code as varchar) as place_of_service_source_value
  from {{ source('raw', 'professional_header_current') }}
),
pharmacy_header_current as (
  select distinct
    cast(
      hash(
        concat_ws(
          '||',
          billing_provider_last_name,
          billing_provider_fein,
          billing_provider_primary_1,
          billing_provider_city,
          billing_provider_state_code,
          billing_provider_postal_code
        )
      , 'xxhash64'
      ) % 1000000000
    as varchar) as care_site_id,
    coalesce(facility_name, billing_provider_last_name) as care_site_name,
    38004338 as place_of_service_concept_id,
    cast(
      hash(
        concat_ws(
          '||',
          billing_provider_last_name,
          billing_provider_fein,
          billing_provider_primary_1,
          billing_provider_city,
          billing_provider_state_code,
          billing_provider_postal_code
        )
      , 'xxhash64'
      ) % 1000000000
    as varchar) as location_id,
    cast(null as varchar) as care_site_source_value,
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
    cast(
      hash(
        concat_ws(
          '||',
          billing_provider_last_name,
          facility_primary_address,
          facility_city,
          facility_state_code,
          facility_postal_code,
          facility_country_code
        )
      , 'xxhash64'
      ) % 1000000000
    as varchar) as care_site_id,
    case
      when LENGTH(billing_provider_state_code) > 2
      then billing_provider_state_code
      else billing_provider_last_name
    end as care_site_name,
    8717 as place_of_service_concept_id,
    cast(
      hash(
        concat_ws(
          '||',
          billing_provider_last_name,
          facility_primary_address,
          facility_city,
          facility_state_code,
          facility_postal_code,
          facility_country_code
        )
      , 'xxhash64'
      ) % 1000000000
    as varchar) as location_id,
    cast(null as varchar) as care_site_source_value,
    cast(null as varchar) as place_of_service_source_value
  from {{ source('raw', 'institutional_header_historical') }}
),
professional_header_historical as (
  select distinct
    cast(
      hash(
        concat_ws(
          '||',
          billing_provider_last_name,
          facility_primary_address,
          facility_city,
          facility_state_code,
          facility_postal_code,
          facility_country_code
        )
      , 'xxhash64'
      ) % 1000000000
    as varchar) as care_site_id,
    billing_provider_last_name as care_site_name,
    8716 as place_of_service_concept_id,
    cast(
      hash(
        concat_ws(
          '||',
          billing_provider_last_name,
          facility_primary_address,
          facility_city,
          facility_state_code,
          facility_postal_code,
          facility_country_code
        )
      , 'xxhash64'
      ) % 1000000000
    as varchar) as location_id,
    cast(null as varchar) as care_site_source_value,
    cast(place_of_service_bill_code as varchar) as place_of_service_source_value
  from {{ source('raw', 'professional_header_historical') }}
),
pharmacy_header_historical as (
  select distinct
    cast(
      hash(
        concat_ws(
          '||',
          billing_provider_last_name,
          billing_provider_fein,
          billing_provider_primary_1,
          billing_provider_city,
          billing_provider_state_code,
          billing_provider_postal_code
        )
      , 'xxhash64'
      ) % 1000000000
    as varchar) as care_site_id,
    coalesce(facility_name, billing_provider_last_name) as care_site_name,
    38004338 as place_of_service_concept_id,
    cast(
      hash(
        concat_ws(
          '||',
          billing_provider_last_name,
          billing_provider_fein,
          billing_provider_primary_1,
          billing_provider_city,
          billing_provider_state_code,
          billing_provider_postal_code
        )
      , 'xxhash64'
      ) % 1000000000
    as varchar) as location_id,
    cast(null as varchar) as care_site_source_value,
    cast(place_of_service_bill_code as varchar) as place_of_service_source_value
  from {{ source('raw', 'pharmacy_header_historical') }}
)
  {% endset %}
  {% do cte_queries.append(query) %}
{% endif %}

{% if has_current or has_historical %}
with {{ cte_queries | join(",\n") }}

select *
from (
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
) as final_result
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
