with base as (
    select distinct
        ndc_billed_code,
        drug_name,
        dispensed_as_written_code,
        basis_of_cost_determination
    from {{ ref('stg_pharmacy_detail') }}
)

select
    ndc_billed_code,
    drug_name,
    dispensed_as_written_code,
    basis_of_cost_determination
from base