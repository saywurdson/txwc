with base as (
    select distinct
        insurer_fein,
        insurer_postal_code,
        claim_administrator_fein,
        claim_administrator_name,
        claim_administrator_postal
    from {{ ref('stg_pharmacy_header') }}

    union

    select distinct
        insurer_fein,
        insurer_postal_code,
        claim_administrator_fein,
        claim_administrator_name,
        claim_administrator_postal
    from {{ ref('stg_institutional_header') }}

    union

    select distinct
        insurer_fein,
        insurer_postal_code,
        claim_administrator_fein,
        claim_administrator_name,
        claim_administrator_postal
    from {{ ref('stg_professional_header') }}
)

select
    insurer_fein,
    insurer_postal_code,
    claim_administrator_fein,
    claim_administrator_name,
    claim_administrator_postal
from base