{% set exists_current = check_table_exists('raw', 'pharmacy_detail_current') %}
{% set exists_historical = check_table_exists('raw', 'pharmacy_detail_historical') %}

with 
{% if exists_current %}
pharmacy_detail_current as (
    select
        row_id,
        created_at,
        updated_at,
        version,
        bill_selection_date,
        bill_id,
        bill_detail_id,
        line_number,
        ndc_billed_code,
        prescription_line_number,
        dispensed_as_written_code,
        drug_name,
        basis_of_cost_determination,
        service_line_from_date,
        service_line_to_date,
        prescription_line_date,
        drugs_supplies_quantity,
        drugs_supplies_number_of,
        drugs_supplies_dispensing,
        drugs_supplies_billed_amount,
        total_amount_paid_per_line,
        ndc_paid_code,
        days_units_paid,
        rendering_line_provider,
        number_of_service_adjustments,
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
        service_adjustment_units_4,
        service_adjustment_group_5,
        service_adjustment_reason_5,
        service_adjustment_amount_5,
        service_adjustment_units_5
    from {{ source('raw', 'pharmacy_detail_current') }}
)
{% endif %}

{% if exists_historical %}
{% if exists_current %}, {% endif %}
pharmacy_detail_historical as (
    select
        row_id,
        created_at,
        updated_at,
        version,
        bill_selection_date,
        bill_id,
        bill_detail_id,
        line_number,
        ndc_billed_code,
        prescription_line_number,
        dispensed_as_written_code,
        drug_name,
        basis_of_cost_determination,
        service_line_from_date,
        service_line_to_date,
        prescription_line_date,
        drugs_supplies_quantity,
        drugs_supplies_number_of,
        drugs_supplies_dispensing,
        drugs_supplies_billed_amount,
        total_amount_paid_per_line,
        ndc_paid_code,
        days_units_paid,
        rendering_line_provider,
        number_of_service_adjustments,
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
        service_adjustment_units_4,
        service_adjustment_group_5,
        service_adjustment_reason_5,
        service_adjustment_amount_5,
        service_adjustment_units_5
    from {{ source('raw', 'pharmacy_detail_historical') }}
)
{% endif %}

select * 
from
    {% if exists_current and exists_historical %}
            pharmacy_detail_current
            union
            select * from pharmacy_detail_historical
        {% elif exists_current %}
            pharmacy_detail_current
        {% elif exists_historical %}
            pharmacy_detail_historical
        {% endif %}