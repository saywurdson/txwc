{% set has_current = check_table_exists('raw', 'institutional_header_current') %}
{% set has_historical = check_table_exists('raw', 'institutional_header_historical') %}

{% set cte_queries = [] %}

{% if has_current %}
  {% set query %}
institutional_header_current as (
  select distinct
    {{ derive_facility_location_id() }} as location_id,
    facility_primary_address as address_1,
    cast(null as varchar) as address_2,
    facility_city as city,
    facility_state_code as state,
    facility_postal_code as zip,
    cast(null as varchar) as county,
    cast(null as varchar) as location_source_value,
    42046186 as country_concept_id,
    facility_country_code as country_source_value,
    cast(null as float) as latitude,
    cast(null as float) as longitude
  from {{ source('raw', 'institutional_header_current') }}

  union

  select distinct
    {{ derive_employee_location_id() }} as location_id,
    cast(null as varchar) as address_1,
    cast(null as varchar) as address_2,
    employee_mailing_city as city,
    employee_mailing_state_code as state,
    employee_mailing_postal_code as zip,
    cast(null as varchar) as county,
    cast(null as varchar) as location_source_value,
    42046186 as country_concept_id,
    employee_mailing_country as country_source_value,
    cast(null as float) as latitude,
    cast(null as float) as longitude
  from {{ source('raw', 'institutional_header_current') }}
),
professional_header_current as (
  select distinct
    {{ derive_facility_location_id() }} as location_id,
    facility_primary_address as address_1,
    facility_secondary_address as address_2,
    facility_city as city,
    facility_state_code as state,
    facility_postal_code as zip,
    cast(null as varchar) as county,
    cast(null as varchar) as location_source_value,
    42046186 as country_concept_id,
    facility_country_code as country_source_value,
    cast(null as float) as latitude,
    cast(null as float) as longitude
  from {{ source('raw', 'professional_header_current') }}

  union

  select distinct
    {{ derive_employee_location_id() }} as location_id,
    cast(null as varchar) as address_1,
    cast(null as varchar) as address_2,
    employee_mailing_city as city,
    employee_mailing_state_code as state,
    employee_mailing_postal_code as zip,
    cast(null as varchar) as county,
    cast(null as varchar) as location_source_value,
    42046186 as country_concept_id,
    employee_mailing_country as country_source_value,
    cast(null as float) as latitude,
    cast(null as float) as longitude
  from {{ source('raw', 'professional_header_current') }}
),
pharmacy_header_current as (
  select distinct
    {{ derive_facility_location_id() }} as location_id,
    facility_primary_address as address_1,
    cast(null as varchar) as address_2,
    facility_city as city,
    facility_state_code as state,
    facility_postal_code as zip,
    cast(null as varchar) as county,
    cast(null as varchar) as location_source_value,
    42046186 as country_concept_id,
    facility_country_code as country_source_value,
    cast(null as float) as latitude,
    cast(null as float) as longitude
  from {{ source('raw', 'pharmacy_header_current') }}

  union

  select distinct
    {{ derive_employee_location_id() }} as location_id,
    cast(null as varchar) as address_1,
    cast(null as varchar) as address_2,
    employee_mailing_city as city,
    employee_mailing_state_code as state,
    employee_mailing_postal_code as zip,
    cast(null as varchar) as county,
    cast(null as varchar) as location_source_value,
    42046186 as country_concept_id,
    employee_mailing_country as country_source_value,
    cast(null as float) as latitude,
    cast(null as float) as longitude
  from {{ source('raw', 'pharmacy_header_current') }}
)
  {% endset %}
  {% do cte_queries.append(query) %}
{% endif %}

{% if has_historical %}
  {% set query %}
institutional_header_historical as (
  select distinct
    {{ derive_facility_location_id() }} as location_id,
    facility_primary_address as address_1,
    cast(null as varchar) as address_2,
    facility_city as city,
    facility_state_code as state,
    facility_postal_code as zip,
    cast(null as varchar) as county,
    cast(null as varchar) as location_source_value,
    42046186 as country_concept_id,
    facility_country_code as country_source_value,
    cast(null as float) as latitude,
    cast(null as float) as longitude
  from {{ source('raw', 'institutional_header_historical') }}

  union

  select distinct
    {{ derive_employee_location_id() }} as location_id,
    cast(null as varchar) as address_1,
    cast(null as varchar) as address_2,
    employee_mailing_city as city,
    employee_mailing_state_code as state,
    employee_mailing_postal_code as zip,
    cast(null as varchar) as county,
    cast(null as varchar) as location_source_value,
    42046186 as country_concept_id,
    employee_mailing_country as country_source_value,
    cast(null as float) as latitude,
    cast(null as float) as longitude
  from {{ source('raw', 'institutional_header_historical') }}
),
professional_header_historical as (
  select distinct
    {{ derive_facility_location_id() }} as location_id,
    facility_primary_address as address_1,
    facility_secondary_address as address_2,
    facility_city as city,
    facility_state_code as state,
    facility_postal_code as zip,
    cast(null as varchar) as county,
    cast(null as varchar) as location_source_value,
    42046186 as country_concept_id,
    facility_country_code as country_source_value,
    cast(null as float) as latitude,
    cast(null as float) as longitude
  from {{ source('raw', 'professional_header_historical') }}

  union

  select distinct
    {{ derive_employee_location_id() }} as location_id,
    cast(null as varchar) as address_1,
    cast(null as varchar) as address_2,
    employee_mailing_city as city,
    employee_mailing_state_code as state,
    employee_mailing_postal_code as zip,
    cast(null as varchar) as county,
    cast(null as varchar) as location_source_value,
    42046186 as country_concept_id,
    employee_mailing_country as country_source_value,
    cast(null as float) as latitude,
    cast(null as float) as longitude
  from {{ source('raw', 'professional_header_historical') }}
),
pharmacy_header_historical as (
  select distinct
    {{ derive_facility_location_id() }} as location_id,
    facility_primary_address as address_1,
    cast(null as varchar) as address_2,
    facility_city as city,
    facility_state_code as state,
    facility_postal_code as zip,
    cast(null as varchar) as county,
    cast(null as varchar) as location_source_value,
    42046186 as country_concept_id,
    facility_country_code as country_source_value,
    cast(null as float) as latitude,
    cast(null as float) as longitude
  from {{ source('raw', 'pharmacy_header_historical') }}

  union

  select distinct
    {{ derive_employee_location_id() }} as location_id,
    cast(null as varchar) as address_1,
    cast(null as varchar) as address_2,
    employee_mailing_city as city,
    employee_mailing_state_code as state,
    employee_mailing_postal_code as zip,
    cast(null as varchar) as county,
    cast(null as varchar) as location_source_value,
    42046186 as country_concept_id,
    employee_mailing_country as country_source_value,
    cast(null as float) as latitude,
    cast(null as float) as longitude
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
    union
    select * from professional_header_current
    union
    select * from pharmacy_header_current
  {% endif %}
  {% if has_current and has_historical %}
    union
  {% endif %}
  {% if has_historical %}
    select * from institutional_header_historical
    union
    select * from professional_header_historical
    union
    select * from pharmacy_header_historical
  {% endif %}
) as final_result
{% else %}
-- No source tables available - return empty result set with OMOP location schema
select
    cast(null as varchar) as location_id,
    cast(null as varchar) as address_1,
    cast(null as varchar) as address_2,
    cast(null as varchar) as city,
    cast(null as varchar) as state,
    cast(null as varchar) as zip,
    cast(null as varchar) as county,
    cast(null as varchar) as location_source_value,
    cast(null as integer) as country_concept_id,
    cast(null as varchar) as country_source_value,
    cast(null as float) as latitude,
    cast(null as float) as longitude
where false
{% endif %}
