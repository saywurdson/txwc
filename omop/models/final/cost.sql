select
    row_number() over (order by c.cost_id) as cost_id,
    case
        when c.cost_domain_id = 'Visit' then vom.visit_occurrence_id
        when c.cost_domain_id = 'Visit Detail' then vdm.visit_detail_id
        else cast(c.cost_event_id as integer)
    end as cost_event_id,
    c.cost_domain_id,
    cast(c.cost_type_concept_id as integer) as cost_type_concept_id,
    cast(c.currency_concept_id as integer) as currency_concept_id,
    c.total_charge,
    c.total_cost,
    c.total_paid,
    c.paid_by_payer,
    c.paid_by_patient,
    c.paid_patient_copay,
    c.paid_patient_coinsurance,
    c.paid_patient_deductible,
    c.paid_by_primary,
    c.paid_ingredient_cost,
    c.paid_dispensing_fee,
    c.payer_plan_period_id,
    c.amount_allowed,
    cast(c.revenue_code_concept_id as integer) as revenue_code_concept_id,
    c.revenue_code_source_value,
    cast(c.drg_concept_id as integer) as drg_concept_id,
    c.drg_source_value
from {{ ref('int_cost') }} c
left join {{ ref('int_visit_occurrence_id_map') }} vom
    on cast(c.cost_event_id as integer) = vom.source_id and c.cost_domain_id = 'Visit'
left join {{ ref('int_visit_detail_id_map') }} vdm
    on cast(c.cost_event_id as integer) = vdm.source_id and c.cost_domain_id = 'Visit Detail'
