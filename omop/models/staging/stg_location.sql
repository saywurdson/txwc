{% set exists_i_current = check_table_exists('raw', 'institutional_header_current') %}
{% set exists_i_historical = check_table_exists('raw', 'institutional_header_historical') %}
{% set exists_pr_current = check_table_exists('raw', 'professional_header_current') %}
{% set exists_pr_historical = check_table_exists('raw', 'professional_header_historical') %}
{% set exists_ph_current = check_table_exists('raw', 'pharmacy_header_current') %}
{% set exists_ph_historical = check_table_exists('raw', 'pharmacy_header_historical') %}

with
{% if exists_i_current %}
institutional_header_current as (
    select distinct
        cast(
            hash(
                concat_ws(
                '||',
                facility_name,
                facility_primary_address,
                facility_city,
                facility_state_code,
                facility_postal_code,
                facility_country_code
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as location_id,
        facility_primary_address as address_1,
        facility_secondary_address as address_2,
        facility_city as city,
        facility_state_code as state,
        facility_postal_code as zip,
        cast(null as varchar) as county,
        facility_national_provider as location_source_value,
        42046186 as country_concept_id,
        facility_country_code as country_source_value,
        cast(null as float) as latitude,
        cast(null as float) as longitude
    from {{ source('raw', 'institutional_header_current') }}

    union

    select distinct
        cast(
            hash(
                concat_ws(
                '||',
                employee_mailing_city,
                employee_mailing_state_code,
                employee_mailing_postal_code,
                employee_mailing_country
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as location_id,
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
)
{% endif %}

{% if exists_i_historical %}
{% if exists_i_current %}, {% endif %}
institutional_header_historical as (
    select distinct
        cast(
            hash(
                concat_ws(
                '||',
                facility_name,
                facility_primary_address,
                facility_city,
                facility_state_code,
                facility_postal_code,
                facility_country_code
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as location_id,
        facility_primary_address as address_1,
        facility_secondary_address as address_2,
        facility_city as city,
        facility_state_code as state,
        facility_postal_code as zip,
        cast(null as varchar) as county,
        facility_national_provider as location_source_value,
        42046186 as country_concept_id,
        facility_country_code as country_source_value,
        cast(null as float) as latitude,
        cast(null as float) as longitude
    from {{ source('raw', 'institutional_header_historical') }}

    union

    select distinct
        cast(
            hash(
                concat_ws(
                '||',
                employee_mailing_city,
                employee_mailing_state_code,
                employee_mailing_postal_code,
                employee_mailing_country
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as location_id,
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
)
{% endif %}

{% if exists_pr_historical %}
{% if exists_i_historical %}, {% endif %}
professional_header_historical as (
    select distinct
        cast(
            hash(
                concat_ws(
                '||',
                facility_name,
                facility_primary_address,
                facility_city,
                facility_state_code,
                facility_postal_code,
                facility_country_code
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as location_id,
        facility_primary_address as address_1,
        facility_secondary_address as address_2,
        facility_city as city,
        facility_state_code as state,
        facility_postal_code as zip,
        cast(null as varchar) as county,
        facility_national_provider as location_source_value,
        42046186 as country_concept_id,
        facility_country_code as country_source_value,
        cast(null as float) as latitude,
        cast(null as float) as longitude
    from {{ source('raw', 'professional_header_historical') }}

    union

    select distinct
        cast(
            hash(
                concat_ws(
                '||',
                employee_mailing_city,
                employee_mailing_state_code,
                employee_mailing_postal_code,
                employee_mailing_country
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as location_id,
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
)
{% endif %}

{% if exists_pr_current %}
{% if exists_pr_historical %}, {% endif %}
professional_header_current as (
    select distinct
        cast(
            hash(
                concat_ws(
                '||',
                facility_name,
                facility_primary_address,
                facility_city,
                facility_state_code,
                facility_postal_code,
                facility_country_code
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as location_id,
        facility_primary_address as address_1,
        facility_secondary_address as address_2,
        facility_city as city,
        facility_state_code as state,
        facility_postal_code as zip,
        cast(null as varchar) as county,
        facility_national_provider as location_source_value,
        42046186 as country_concept_id,
        facility_country_code as country_source_value,
        cast(null as float) as latitude,
        cast(null as float) as longitude
    from {{ source('raw', 'professional_header_current') }}

    union

    select distinct
        cast(
            hash(
                concat_ws(
                '||',
                employee_mailing_city,
                employee_mailing_state_code,
                employee_mailing_postal_code,
                employee_mailing_country
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as location_id,
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
)
{% endif %}

{% if exists_ph_current %}
{% if exists_pr_current %}, {% endif %}
pharmacy_header_current as (
    select distinct
        cast(
            hash(
                concat_ws(
                '||',
                facility_name,
                facility_primary_address,
                facility_city,
                facility_state_code,
                facility_postal_code,
                facility_country_code
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as location_id,
        facility_primary_address as address_1,
        facility_secondary_address as address_2,
        facility_city as city,
        facility_state_code as state,
        facility_postal_code as zip,
        cast(null as varchar) as county,
        facility_national_provider as location_source_value,
        42046186 as country_concept_id,
        facility_country_code as country_source_value,
        cast(null as float) as latitude,
        cast(null as float) as longitude
    from {{ source('raw', 'pharmacy_header_current') }}

    union

    select distinct
        cast(
            hash(
                concat_ws(
                '||',
                employee_mailing_city,
                employee_mailing_state_code,
                employee_mailing_postal_code,
                employee_mailing_country
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as location_id,
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
{% endif %}

{% if exists_ph_historical %}
{% if exists_ph_current %}, {% endif %}
pharmacy_header_historical as (
    select distinct
        cast(
            hash(
                concat_ws(
                '||',
                facility_name,
                facility_primary_address,
                facility_city,
                facility_state_code,
                facility_postal_code,
                facility_country_code
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as location_id,
        facility_primary_address as address_1,
        facility_secondary_address as address_2,
        facility_city as city,
        facility_state_code as state,
        facility_postal_code as zip,
        cast(null as varchar) as county,
        facility_national_provider as location_source_value,
        42046186 as country_concept_id,
        facility_country_code as country_source_value,
        cast(null as float) as latitude,
        cast(null as float) as longitude
    from {{ source('raw', 'pharmacy_header_historical') }}

    union

    select distinct
        cast(
            hash(
                concat_ws(
                '||',
                employee_mailing_city,
                employee_mailing_country,
                employee_mailing_postal_code,
                employee_mailing_state_code
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as location_id,
        facility_primary_address as address_1,
        facility_secondary_address as address_2,
        facility_city as city,
        facility_state_code as state,
        facility_postal_code as zip,
        cast(null as varchar) as county,
        facility_national_provider as location_source_value,
        42046186 as country_concept_id,
        facility_country_code as country_source_value,
        cast(null as float) as latitude,
        cast(null as float) as longitude
    from {{ source('raw', 'pharmacy_header_historical') }}
)
{% endif %}

{% set cte_list = [] %}
{% if exists_i_current %}
  {% set _ = cte_list.append("select * from institutional_header_current") %}
{% endif %}
{% if exists_i_historical %}
  {% set _ = cte_list.append("select * from institutional_header_historical") %}
{% endif %}
{% if exists_pr_current %}
  {% set _ = cte_list.append("select * from professional_header_current") %}
{% endif %}
{% if exists_pr_historical %}
  {% set _ = cte_list.append("select * from professional_header_historical") %}
{% endif %}
{% if exists_ph_current %}
  {% set _ = cte_list.append("select * from pharmacy_header_current") %}
{% endif %}
{% if exists_ph_historical %}
  {% set _ = cte_list.append("select * from pharmacy_header_historical") %}
{% endif %}

select *
from (
    {{ cte_list | join(" union ") }}
) as final_result