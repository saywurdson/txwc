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
                billing_provider_last_name,
                facility_primary_address,
                facility_city,
                facility_state_code,
                facility_postal_code,
                facility_country_code,
                coalesce(facility_national_provider, '')
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as care_site_id,
        billing_provider_last_name as care_site_name,
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
        billing_provider_last_name as care_site_source_value,
        cast(null as varchar) as place_of_service_source_value
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
                billing_provider_last_name,
                facility_primary_address,
                facility_city,
                facility_state_code,
                facility_postal_code,
                facility_country_code,
                coalesce(facility_national_provider, '')
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as care_site_id,
        billing_provider_last_name as care_site_name,
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
        billing_provider_last_name as care_site_source_value,
        cast(null as varchar) as place_of_service_source_value
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
                billing_provider_last_name,
                facility_primary_address,
                facility_city,
                facility_state_code,
                facility_postal_code,
                facility_country_code,
                coalesce(facility_national_provider, '')
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
        facility_national_provider as care_site_source_value,
        cast(null as varchar) as place_of_service_source_value
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
                billing_provider_last_name,
                facility_primary_address,
                facility_city,
                facility_state_code,
                facility_postal_code,
                facility_country_code,
                coalesce(facility_national_provider, '')
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
        billing_provider_last_name as care_site_source_value,
        cast(null as varchar) as place_of_service_source_value
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
                billing_provider_last_name,
                facility_primary_address,
                facility_city,
                facility_state_code,
                facility_postal_code,
                facility_country_code,
                coalesce(facility_national_provider, '')
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as care_site_id,
        facility_name as care_site_name,
        38004338 as place_of_service_concept_id,
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
        facility_name as care_site_source_value,
        cast(null as varchar) as place_of_service_source_value
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
                billing_provider_last_name,
                facility_primary_address,
                facility_city,
                facility_state_code,
                facility_postal_code,
                facility_country_code,
                coalesce(facility_national_provider, '')
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as care_site_id,
        facility_name as care_site_name,
        38004338 as place_of_service_concept_id,
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
        facility_name as care_site_source_value,
        cast(null as varchar) as place_of_service_source_value
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