select
    row_number() over (order by person_id, payer_plan_period_start_date) as payer_plan_period_id,
    person_id,
    payer_plan_period_start_date,
    payer_plan_period_end_date,
    cast(0 as integer) as payer_concept_id,
    payer_source_value,
    cast(0 as integer) as payer_source_concept_id,
    cast(0 as integer) as plan_concept_id,
    plan_source_value,
    cast(0 as integer) as plan_source_concept_id,
    cast(0 as integer) as sponsor_concept_id,
    sponsor_source_value,
    cast(0 as integer) as sponsor_source_concept_id,
    family_source_value,
    cast(0 as integer) as stop_reason_concept_id,
    stop_reason_source_value,
    cast(0 as integer) as stop_reason_source_concept_id
from {{ ref('int_payer_plan_period') }}
