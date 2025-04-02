select
    cast(cost_id as integer) as cost_id,
    cast(cost_event_id as varchar) as cost_event_id,
    cast(cost_domain_id as integer) as cost_domain_id,
    cast(cost_type_concept_id as varchar) as cost_type_concept_id,
    cast(currency_concept_id as varchar) as currency_concept_id,
    cast(total_charge as float) as total_charge,
    cast(total_cost as float) as total_cost,
    cast(total_paid as float) as total_paid,
    cast(paid_by_payer as float) as paid_by_payer,
    cast(paid_by_patient as float) as paid_by_patient,
    cast(paid_patient_copay as float) as paid_patient_copay,
    cast(paid_patient_coinsurance as float) as paid_patient_coinsurance,
    cast(paid_patient_deductible as float) as paid_patient_deductible,
    cast(paid_by_primary as float) as paid_by_primary,
    cast(paid_ingredient_cost as float) as paid_ingredient_cost,
    cast(paid_dispensing_fee as float) as paid_dispensing_fee,
    cast(payer_plan_period_id as integer) as payer_plan_period_id,
    cast(amount_allowed as float) as amount_allowed,
    cast({{ get_source_concept_ids(
        "revenue_code_source_value",
        domain_id='Revenue Code',
        vocabulary_id=['Revenue Code'],
        standard_concept='S',
        invalid_reason='is null',
        required_value=0
    ) }} as varchar) as revenue_code_concept_id,
    cast(revenue_code_source_value as varchar) as revenue_code_source_value,
    cast({{ get_source_concept_ids(
        "drg_source_value",
        domain_id='Observation',
        vocabulary_id=['DRG'],
        standard_concept='S',
        invalid_reason='is null',
        required_value=0
    ) }} as varchar) as drg_concept_id,
    cast(drg_source_value as varchar) as drg_source_value
from {{ ref('stg_cost') }}