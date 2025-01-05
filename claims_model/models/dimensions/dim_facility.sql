with base as (
    select
        facility_fein,
        facility_name,
        facility_primary_address,
        facility_secondary_address,
        facility_city,
        facility_state_code,
        facility_postal_code,
        facility_country_code,
        facility_state_license_number,
        facility_medicare_number,
        facility_national_provider
    from {{ ref('stg_pharmacy_header') }}

    union

    select
        facility_fein,
        facility_name,
        facility_primary_address,
        facility_secondary_address,
        facility_city,
        facility_state_code,
        facility_postal_code,
        facility_country_code,
        facility_state_license_number,
        facility_medicare_number,
        facility_national_provider
    from {{ ref('stg_institutional_header') }}

    union

    select
        facility_fein,
        facility_name,
        facility_primary_address,
        facility_secondary_address,
        facility_city,
        facility_state_code,
        facility_postal_code,
        facility_country_code,
        facility_state_license_number,
        facility_medicare_number,
        facility_national_provider
    from {{ ref('stg_professional_header') }}
)

select distinct
    facility_fein,
    facility_name,
    facility_primary_address,
    facility_secondary_address,
    facility_city,
    facility_state_code,
    facility_postal_code,
    facility_country_code,
    facility_state_license_number,
    facility_medicare_number,
    facility_national_provider
from base