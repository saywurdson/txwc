{% set exists_current = check_table_exists('raw', 'institutional_detail_current') %}
{% set exists_historical = check_table_exists('raw', 'institutional_detail_historical') %}

with 
{% if exists_current %}
institutional_detail_current as (
    select
        bill_selection_date,
        bill_id,
        bill_detail_id,
        line_number,
        hcpcs_line_procedure_billed,
        first_hcpcs_modifier_billed,
        second_hcpcs_modifier_billed,
        third_hcpcs_modifier_billed,
        fourth_hcpcs_modifier_billed,
        procedure_description,
        total_charge_per_line,
        days_units_code,
        days_units_billed,
        place_of_service_line_code,
        provider_agreement_line_code,
        revenue_billed_code,
        revenue_unit_rate,
        service_line_from_date,
        service_line_to_date,
        contract_line_type_code,
        treatment_line_authorization,
        total_amount_paid_per_line,
        hcpcs_line_procedure_paid,
        first_hcpcs_modifier_paid,
        second_hcpcs_modifier_paid,
        third_hcpcs_modifier_paid,
        fourth_hcpcs_modifier_paid,
        revenue_paid_code,
        days_units_paid,
        rendering_line_provider,
        number_of_service_adjustments,
        service_adjustment_group,
        service_adjustment_reason,
        service_adjustment_amount,
        service_adjustment_units,
        service_adjustment_group_1,
        service_adjustment_reason_1,
        service_adjustment_amount_1,
        service_adjustment_units_1,
        service_adjustment_group_2,
        service_adjustment_reason_2,
        service_adjustment_amount_2,
        service_adjustment_units_2,
        service_adjustment_group_3,
        service_adjustment_reason_3,
        service_adjustment_amount_3,
        service_adjustment_units_3,
        service_adjustment_group_4,
        service_adjustment_reason_4,
        service_adjustment_amount_4,
        service_adjustment_units_4
    from {{ source('raw', 'institutional_detail_current') }}
)
{% endif %}

{% if exists_historical %}
{% if exists_current %}, {% endif %}
institutional_detail_historical as (
    select
        bill_selection_date,
        bill_id,
        bill_detail_id,
        line_number,
        hcpcs_line_procedure_billed,
        first_hcpcs_modifier_billed,
        second_hcpcs_modifier_billed,
        third_hcpcs_modifier_billed,
        fourth_hcpcs_modifier_billed,
        procedure_description,
        total_charge_per_line,
        days_units_code,
        days_units_billed,
        place_of_service_line_code,
        provider_agreement_line_code,
        revenue_billed_code,
        revenue_unit_rate,
        service_line_from_date,
        service_line_to_date,
        contract_line_type_code,
        treatment_line_authorization,
        total_amount_paid_per_line,
        hcpcs_line_procedure_paid,
        first_hcpcs_modifier_paid,
        second_hcpcs_modifier_paid,
        third_hcpcs_modifier_paid,
        fourth_hcpcs_modifier_paid,
        revenue_paid_code,
        days_units_paid,
        rendering_line_provider,
        number_of_service_adjustments,
        service_adjustment_group,
        service_adjustment_reason,
        service_adjustment_amount,
        service_adjustment_units,
        service_adjustment_group_1,
        service_adjustment_reason_1,
        service_adjustment_amount_1,
        service_adjustment_units_1,
        service_adjustment_group_2,
        service_adjustment_reason_2,
        service_adjustment_amount_2,
        service_adjustment_units_2,
        service_adjustment_group_3,
        service_adjustment_reason_3,
        service_adjustment_amount_3,
        service_adjustment_units_3,
        service_adjustment_group_4,
        service_adjustment_reason_4,
        service_adjustment_amount_4,
        service_adjustment_units_4
    from {{ source('raw', 'institutional_detail_historical') }}
)
{% endif %}

select * 
from
    {% if exists_current and exists_historical %}
            institutional_detail_current
            union
            select * from institutional_detail_historical
        {% elif exists_current %}
            institutional_detail_current
        {% elif exists_historical %}
            institutional_detail_historical
        {% endif %}