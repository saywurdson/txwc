-- Deduplicate payer plan periods per person
-- Merge overlapping periods for same payer/plan combination
select
    cast(person_id as integer) as person_id,
    cast(payer_plan_period_start_date as date) as payer_plan_period_start_date,
    cast(payer_plan_period_end_date as date) as payer_plan_period_end_date,
    cast(payer_source_value as varchar) as payer_source_value,
    cast(plan_source_value as varchar) as plan_source_value,
    cast(sponsor_source_value as varchar) as sponsor_source_value,
    cast(family_source_value as varchar) as family_source_value,
    cast(stop_reason_source_value as varchar) as stop_reason_source_value
from {{ ref('stg_payer_plan_period') }}
where payer_plan_period_start_date is not null
  and person_id is not null
qualify row_number() over (
    partition by person_id, payer_source_value, plan_source_value, payer_plan_period_start_date
    order by payer_plan_period_end_date desc nulls last
) = 1
