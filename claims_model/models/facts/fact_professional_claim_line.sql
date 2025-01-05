with base as (
    select
        fb.bill_id,
        det.bill_detail_id,
        det.line_number,
        det.hcpcs_line_procedure_billed,
        det.first_hcpcs_modifier_billed,
        det.second_hcpcs_modifier_billed,
        det.third_hcpcs_modifier_billed,
        det.fourth_hcpcs_modifier_billed,
        det.procedure_description,
        det.total_charge_per_line,
        det.days_units_code,
        det.days_units_billed,
        det.place_of_service_line_code,
        det.provider_agreement_line_code,
        det.service_line_from_date,
        det.service_line_to_date,
        det.contract_line_type_code,
        det.treatment_line_authorization,
        det.total_amount_paid_per_line,
        det.hcpcs_line_procedure_paid,
        det.first_hcpcs_modifier_paid,
        det.second_hcpcs_modifier_paid,
        det.third_hcpcs_modifier_paid,
        det.fourth_hcpcs_modifier_paid,
        det.days_units_paid,
        det.rendering_line_provider,
        det.number_of_service_adjustments,
        det.service_adjustment_group,
        det.service_adjustment_reason,
        det.service_adjustment_amount,
        det.service_adjustment_units
    from {{ ref('stg_professional_detail') }} det
    left join {{ ref('fact_professional_claim') }} fb
        on det.bill_id = fb.bill_id
)

select * from base
