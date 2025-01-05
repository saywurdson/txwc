with base as (
    select distinct
        coalesce(billing_provider_fein, rendering_bill_provider_fein, referring_provider_fein) as provider_fein,
        case
            when billing_provider_fein is not null then 'Billing'
            when rendering_bill_provider_fein is not null then 'Rendering'
            when referring_provider_fein is not null then 'Referring'
            else 'Other'
        end as provider_type,
        coalesce(billing_provider_last_name, rendering_bill_provider_last, referring_provider_last_name) as provider_last_name,
        coalesce(billing_provider_first_name, rendering_bill_provider_first, referring_provider_first) as provider_first_name,
        coalesce(billing_provider_middle_name, rendering_bill_provider_middle, referring_provider_middle) as provider_middle_name,
        coalesce(billing_provider_suffix, rendering_bill_provider_suffix, referring_provider_suffix) as provider_suffix,
        coalesce(billing_provider_gate_keeper, rendering_bill_provider_gate, referring_provider_gate_keeper) as provider_gate_keeper,
        coalesce(billing_provider_city, rendering_bill_provider_city) as provider_city,
        coalesce(billing_provider_state_code, rendering_bill_provider_state, referring_provider_state) as provider_state_code,
        coalesce(billing_provider_postal_code, rendering_bill_provider_postal) as provider_postal_code,
        coalesce(billing_provider_country, rendering_bill_provider_country) as provider_country,
        coalesce(billing_provider_state_license, rendering_bill_provider_state_license) as provider_state_license,
        coalesce(billing_provider_medicare, rendering_bill_provider_medicare, referring_provider_medicare) as provider_medicare,
        coalesce(billing_provider_national, rendering_bill_provider_national, referring_provider_national) as provider_national,
        referring_provider_specialty as provider_specialty
    from {{ ref('stg_pharmacy_header') }}

    union

    select distinct
        coalesce(billing_provider_fein, rendering_bill_provider_fein, referring_provider_fein) as provider_fein,
        case
            when billing_provider_fein is not null then 'Billing'
            when rendering_bill_provider_fein is not null then 'Rendering'
            when referring_provider_fein is not null then 'Referring'
            else 'Other'
        end as provider_type,
        coalesce(billing_provider_last_name, rendering_bill_provider_last, referring_provider_last_name) as provider_last_name,
        coalesce(billing_provider_first_name, rendering_bill_provider_first, referring_provider_first) as provider_first_name,
        coalesce(billing_provider_middle_name, rendering_bill_provider_middle, referring_provider_middle) as provider_middle_name,
        coalesce(billing_provider_suffix, rendering_bill_provider_suffix, referring_provider_suffix) as provider_suffix,
        coalesce(billing_provider_gate_keeper, rendering_bill_provider_gate, referring_provider_gate_keeper) as provider_gate_keeper,
        coalesce(billing_provider_city, rendering_bill_provider_city) as provider_city,
        coalesce(billing_provider_state_code, rendering_bill_provider_state, referring_provider_state) as provider_state_code,
        coalesce(billing_provider_postal_code, rendering_bill_provider_postal) as provider_postal_code,
        coalesce(billing_provider_country, rendering_bill_provider_country) as provider_country,
        coalesce(billing_provider_state_license, rendering_bill_provider_state_license) as provider_state_license,
        coalesce(billing_provider_medicare, rendering_bill_provider_medicare, referring_provider_medicare) as provider_medicare,
        coalesce(billing_provider_national, rendering_bill_provider_national, referring_provider_national) as provider_national,
        referring_provider_specialty as provider_specialty
    from {{ ref('stg_institutional_header') }}

    union

    select distinct
        coalesce(billing_provider_fein, rendering_bill_provider_fein, referring_provider_fein) as provider_fein,
        case
            when billing_provider_fein is not null then 'Billing'
            when rendering_bill_provider_fein is not null then 'Rendering'
            when referring_provider_fein is not null then 'Referring'
            else 'Other'
        end as provider_type,
        coalesce(billing_provider_last_name, rendering_bill_provider_last, referring_provider_last_name) as provider_last_name,
        coalesce(billing_provider_first_name, rendering_bill_provider_first, referring_provider_first) as provider_first_name,
        coalesce(billing_provider_middle_name, rendering_bill_provider_middle, referring_provider_middle) as provider_middle_name,
        coalesce(billing_provider_suffix, rendering_bill_provider_suffix, referring_provider_suffix) as provider_suffix,
        coalesce(billing_provider_gate_keeper, rendering_bill_provider_gate, referring_provider_gate_keeper) as provider_gate_keeper,
        coalesce(billing_provider_city, rendering_bill_provider_city) as provider_city,
        coalesce(billing_provider_state_code, rendering_bill_provider_state, referring_provider_state) as provider_state_code,
        coalesce(billing_provider_postal_code, rendering_bill_provider_postal) as provider_postal_code,
        coalesce(billing_provider_country, rendering_bill_provider_country) as provider_country,
        coalesce(billing_provider_state_license, rendering_bill_provider_state_license) as provider_state_license,
        coalesce(billing_provider_medicare, rendering_bill_provider_medicare, referring_provider_medicare) as provider_medicare,
        coalesce(billing_provider_national, rendering_bill_provider_national, referring_provider_national) as provider_national,
        referring_provider_specialty as provider_specialty
    from {{ ref('stg_professional_header') }}
)

select
    provider_fein,
    provider_type,
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
from base