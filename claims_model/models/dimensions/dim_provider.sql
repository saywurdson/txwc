with billing_providers_pharmacy as (
    select
        'billing' as provider_type,
        billing_provider_fein as provider_fein,
        billing_provider_last_name as provider_last_name,
        billing_provider_first_name as provider_first_name,
        billing_provider_middle_name as provider_middle_name,
        billing_provider_suffix as provider_suffix,
        billing_provider_gate_keeper as provider_gate_keeper,
        billing_provider_city as provider_city,
        billing_provider_state_code as provider_state_code,
        billing_provider_postal_code as provider_postal_code,
        billing_provider_country as provider_country,
        billing_provider_state_license as provider_state_license,
        billing_provider_medicare as provider_medicare,
        billing_provider_national as provider_national,
        null as provider_specialty
    from {{ ref('stg_pharmacy_header') }}
),

rendering_providers_pharmacy as (
    select
        'rendering' as provider_type,
        rendering_bill_provider_fein as provider_fein,
        rendering_bill_provider_last as provider_last_name,
        rendering_bill_provider_first as provider_first_name,
        rendering_bill_provider_middle as provider_middle_name,
        rendering_bill_provider_suffix as provider_suffix,
        rendering_bill_provider_gate as provider_gate_keeper,
        rendering_bill_provider_city as provider_city,
        rendering_bill_provider_state as provider_state_code,
        rendering_bill_provider_postal as provider_postal_code,
        null as provider_country,
        null as provider_state_license,
        null as provider_medicare,
        null as provider_national,
        null as provider_specialty
    from {{ ref('stg_pharmacy_header') }}
),

referring_providers_pharmacy as (
    select
        'referring' as provider_type,
        referring_provider_fein as provider_fein,
        referring_provider_last_name as provider_last_name,
        referring_provider_first as provider_first_name,
        referring_provider_middle as provider_middle_name,
        referring_provider_suffix as provider_suffix,
        referring_provider_gate_keeper as provider_gate_keeper,
        null as provider_city,
        referring_provider_state as provider_state_code,
        null as provider_postal_code,
        null as provider_country,
        null as provider_state_license,
        referring_provider_medicare as provider_medicare,
        referring_provider_national as provider_national,
        referring_provider_specialty as provider_specialty
    from {{ ref('stg_pharmacy_header') }}
),

billing_providers_institutional as (
    select
        'billing' as provider_type,
        billing_provider_fein as provider_fein,
        billing_provider_last_name as provider_last_name,
        billing_provider_first_name as provider_first_name,
        billing_provider_middle_name as provider_middle_name,
        billing_provider_suffix as provider_suffix,
        billing_provider_gate_keeper as provider_gate_keeper,
        billing_provider_city as provider_city,
        billing_provider_state_code as provider_state_code,
        billing_provider_postal_code as provider_postal_code,
        billing_provider_country as provider_country,
        billing_provider_state_license as provider_state_license,
        billing_provider_medicare as provider_medicare,
        billing_provider_national as provider_national,
        null as provider_specialty
    from {{ ref('stg_institutional_header') }}
),

rendering_providers_institutional as (
    select
        'rendering' as provider_type,
        rendering_bill_provider_fein as provider_fein,
        rendering_bill_provider_last as provider_last_name,
        rendering_bill_provider_first as provider_first_name,
        rendering_bill_provider_middle as provider_middle_name,
        rendering_bill_provider_suffix as provider_suffix,
        rendering_bill_provider_gate as provider_gate_keeper,
        rendering_bill_provider_city as provider_city,
        rendering_bill_provider_state as provider_state_code,
        rendering_bill_provider_postal as provider_postal_code,
        null as provider_country,
        null as provider_state_license,
        null as provider_medicare,
        null as provider_national,
        null as provider_specialty
    from {{ ref('stg_institutional_header') }}
),

referring_providers_institutional as (
    select
        'referring' as provider_type,
        referring_provider_fein as provider_fein,
        referring_provider_last_name as provider_last_name,
        referring_provider_first as provider_first_name,
        referring_provider_middle as provider_middle_name,
        referring_provider_suffix as provider_suffix,
        referring_provider_gate_keeper as provider_gate_keeper,
        null as provider_city,
        referring_provider_state as provider_state_code,
        null as provider_postal_code,
        null as provider_country,
        null as provider_state_license,
        referring_provider_medicare as provider_medicare,
        referring_provider_national as provider_national,
        referring_provider_specialty as provider_specialty
    from {{ ref('stg_institutional_header') }}
),

billing_providers_professional as (
    select
        'billing' as provider_type,
        billing_provider_fein as provider_fein,
        billing_provider_last_name as provider_last_name,
        billing_provider_first_name as provider_first_name,
        billing_provider_middle_name as provider_middle_name,
        billing_provider_suffix as provider_suffix,
        billing_provider_gate_keeper as provider_gate_keeper,
        billing_provider_city as provider_city,
        billing_provider_state_code as provider_state_code,
        billing_provider_postal_code as provider_postal_code,
        billing_provider_country as provider_country,
        billing_provider_state_license as provider_state_license,
        billing_provider_medicare as provider_medicare,
        billing_provider_national as provider_national,
        null as provider_specialty
    from {{ ref('stg_professional_header') }}
),

rendering_providers_professional as (
    select
        'rendering' as provider_type,
        rendering_bill_provider_fein as provider_fein,
        rendering_bill_provider_last as provider_last_name,
        rendering_bill_provider_first as provider_first_name,
        rendering_bill_provider_middle as provider_middle_name,
        rendering_bill_provider_suffix as provider_suffix,
        rendering_bill_provider_gate as provider_gate_keeper,
        rendering_bill_provider_city as provider_city,
        rendering_bill_provider_state as provider_state_code,
        rendering_bill_provider_postal as provider_postal_code,
        null as provider_country,
        null as provider_state_license,
        null as provider_medicare,
        null as provider_national,
        null as provider_specialty
    from {{ ref('stg_professional_header') }}
),

referring_providers_professional as (
    select
        'referring' as provider_type,
        referring_provider_fein as provider_fein,
        referring_provider_last_name as provider_last_name,
        referring_provider_first as provider_first_name,
        referring_provider_middle as provider_middle_name,
        referring_provider_suffix as provider_suffix,
        referring_provider_gate_keeper as provider_gate_keeper,
        null as provider_city,
        referring_provider_state as provider_state_code,
        null as provider_postal_code,
        null as provider_country,
        null as provider_state_license,
        referring_provider_medicare as provider_medicare,
        referring_provider_national as provider_national,
        referring_provider_specialty as provider_specialty
    from {{ ref('stg_professional_header') }}
),

unioned_providers as (
    select * from billing_providers_pharmacy
    union all
    select * from rendering_providers_pharmacy
    union all
    select * from referring_providers_pharmacy

    union all
    select * from billing_providers_institutional
    union all
    select * from rendering_providers_institutional
    union all
    select * from referring_providers_institutional

    union all
    select * from billing_providers_professional
    union all
    select * from rendering_providers_professional
    union all
    select * from referring_providers_professional
)

select distinct
    provider_type,
    provider_fein,
    provider_last_name,
    provider_first_name,
    provider_middle_name,
    provider_suffix,
    provider_gate_keeper,
    provider_city,
    provider_state_code,
    provider_postal_code,
    provider_country,
    provider_state_license,
    provider_medicare,
    provider_national,
    provider_specialty
from unioned_providers