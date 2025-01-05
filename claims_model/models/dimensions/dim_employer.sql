with base as (
    select
        employer_fein,
        employer_physical_city,
        employer_physical_state_code,
        employer_physical_postal,
        employer_physical_country
    from {{ ref('stg_pharmacy_header') }}

    union

    select
        employer_fein,
        employer_physical_city,
        employer_physical_state_code,
        employer_physical_postal,
        employer_physical_country
    from {{ ref('stg_institutional_header') }}

    union

    select
        employer_fein,
        employer_physical_city,
        employer_physical_state_code,
        employer_physical_postal,
        employer_physical_country
    from {{ ref('stg_professional_header') }}
)

select distinct
    employer_fein,
    employer_physical_city,
    employer_physical_state_code,
    employer_physical_postal,
    employer_physical_country
from base