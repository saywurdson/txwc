{% set has_current = check_table_exists('raw', 'institutional_header_current') %}
{% set has_historical = check_table_exists('raw', 'institutional_header_historical') %}

{% set cte_definitions = [] %}

{% if has_current %}
  {% set query %}
institutional_header_current as (
  select distinct
      cast(
        hash(concat_ws('||',
          h.row_id,
          h.bill_id,
          h.total_charge_per_bill), 'xxhash64') % 1000000000 as varchar
      ) as cost_id,
      cast(h.bill_id as integer) as cost_event_id,
      'Visit' as cost_domain_id,
      32855 as cost_type_concept_id,
      44818668 as currency_concept_id,
      TRY_CAST(h.total_charge_per_bill as float) as total_charge,
      cast(null as float) as total_cost,
      TRY_CAST(h.total_amount_paid_per_bill as float) as total_paid,
      cast(null as float) as paid_by_payer,
      cast(null as float) as paid_by_patient,
      cast(null as float) as paid_patient_copay,
      cast(null as float) as paid_patient_coinsurance,
      cast(null as float) as paid_patient_deductible,
      cast(null as float) as paid_by_primary,
      cast(null as float) as paid_ingredient_cost,
      cast(null as float) as paid_dispensing_fee,
      cast(null as integer) as payer_plan_period_id,
      cast(null as float) as amount_allowed,
      cast(null as integer) as revenue_code_concept_id,
      d.revenue_billed_code as revenue_code_source_value,
      cast(null as integer) as drg_concept_id,
      h.diagnosis_related_group_code as drg_source_value
  from {{ source('raw', 'institutional_header_current') }} h
  join {{ source('raw', 'institutional_detail_current') }} d on h.bill_id = d.bill_id
),
professional_header_current as (
  select distinct
      cast(
        hash(concat_ws('||',
          row_id,
          bill_id,
          total_charge_per_bill), 'xxhash64') % 1000000000 as varchar
      ) as cost_id,
      cast(bill_id as integer) as cost_event_id,
      'Procedure' as cost_domain_id,
      32873 as cost_type_concept_id,
      44818668 as currency_concept_id,
      cast(total_charge_per_bill as float) as total_charge,
      cast(null as float) as total_cost,
      cast(total_amount_paid_per_bill as float) as total_paid,
      cast(null as float) as paid_by_payer,
      cast(null as float) as paid_by_patient,
      cast(null as float) as paid_patient_copay,
      cast(null as float) as paid_patient_coinsurance,
      cast(null as float) as paid_patient_deductible,
      cast(null as float) as paid_by_primary,
      cast(null as float) as paid_ingredient_cost,
      cast(null as float) as paid_dispensing_fee,
      cast(null as integer) as payer_plan_period_id,
      cast(null as float) as amount_allowed,
      cast(null as integer) as revenue_code_concept_id,
      cast(null as varchar) as revenue_code_source_value,
      cast(null as integer) as drg_concept_id,
      cast(null as varchar) as drg_source_value
  from {{ source('raw', 'professional_header_current') }}
),
pharmacy_header_current as (
  select distinct
      cast(
        hash(concat_ws('||',
          row_id,
          bill_id,
          total_charge_per_bill), 'xxhash64') % 1000000000 as varchar
      ) as cost_id,
      cast(bill_id as integer) as cost_event_id,
      'Drug' as cost_domain_id,
      32869 as cost_type_concept_id,
      44818668 as currency_concept_id,
      cast(total_charge_per_bill as float) as total_charge,
      cast(null as float) as total_cost,
      cast(total_amount_paid_per_bill as float) as total_paid,
      cast(null as float) as paid_by_payer,
      cast(null as float) as paid_by_patient,
      cast(null as float) as paid_patient_copay,
      cast(null as float) as paid_patient_coinsurance,
      cast(null as float) as paid_patient_deductible,
      cast(null as float) as paid_by_primary,
      cast(null as float) as paid_ingredient_cost,
      cast(null as float) as paid_dispensing_fee,
      cast(null as integer) as payer_plan_period_id,
      cast(null as float) as amount_allowed,
      cast(null as integer) as revenue_code_concept_id,
      cast(null as varchar) as revenue_code_source_value,
      cast(null as integer) as drg_concept_id,
      cast(null as varchar) as drg_source_value
  from {{ source('raw', 'pharmacy_header_current') }}
)
  {% endset %}
  {% do cte_definitions.append(query) %}
{% endif %}

{% if has_historical %}
  {% set query %}
institutional_header_historical as (
  select distinct
      cast(
        hash(concat_ws('||',
          h.row_id,
          h.bill_id,
          h.total_charge_per_bill), 'xxhash64') % 1000000000 as varchar
      ) as cost_id,
      cast(h.bill_id as integer) as cost_event_id,
      'Visit' as cost_domain_id,
      32855 as cost_type_concept_id,
      44818668 as currency_concept_id,
      TRY_CAST(h.total_charge_per_bill as float) as total_charge,
      cast(null as float) as total_cost,
      TRY_CAST(h.total_amount_paid_per_bill as float) as total_paid,
      cast(null as float) as paid_by_payer,
      cast(null as float) as paid_by_patient,
      cast(null as float) as paid_patient_copay,
      cast(null as float) as paid_patient_coinsurance,
      cast(null as float) as paid_patient_deductible,
      cast(null as float) as paid_by_primary,
      cast(null as float) as paid_ingredient_cost,
      cast(null as float) as paid_dispensing_fee,
      cast(null as integer) as payer_plan_period_id,
      cast(null as float) as amount_allowed,
      cast(null as integer) as revenue_code_concept_id,
      d.revenue_billed_code as revenue_code_source_value,
      cast(null as integer) as drg_concept_id,
      h.diagnosis_related_group_code as drg_source_value
  from {{ source('raw', 'institutional_header_historical') }} h
  join {{ source('raw', 'institutional_detail_historical') }} d on h.bill_id = d.bill_id
),
professional_header_historical as (
  select distinct
      cast(
        hash(concat_ws('||',
          row_id,
          bill_id,
          total_charge_per_bill), 'xxhash64') % 1000000000 as varchar
      ) as cost_id,
      cast(bill_id as integer) as cost_event_id,
      'Procedure' as cost_domain_id,
      32873 as cost_type_concept_id,
      44818668 as currency_concept_id,
      cast(total_charge_per_bill as float) as total_charge,
      cast(null as float) as total_cost,
      cast(total_amount_paid_per_bill as float) as total_paid,
      cast(null as float) as paid_by_payer,
      cast(null as float) as paid_by_patient,
      cast(null as float) as paid_patient_copay,
      cast(null as float) as paid_patient_coinsurance,
      cast(null as float) as paid_patient_deductible,
      cast(null as float) as paid_by_primary,
      cast(null as float) as paid_ingredient_cost,
      cast(null as float) as paid_dispensing_fee,
      cast(null as integer) as payer_plan_period_id,
      cast(null as float) as amount_allowed,
      cast(null as integer) as revenue_code_concept_id,
      cast(null as varchar) as revenue_code_source_value,
      cast(null as integer) as drg_concept_id,
      cast(null as varchar) as drg_source_value
  from {{ source('raw', 'professional_header_historical') }}
),
pharmacy_header_historical as (
  select distinct
      cast(
        hash(concat_ws('||',
          row_id,
          bill_id,
          total_charge_per_bill), 'xxhash64') % 1000000000 as varchar
      ) as cost_id,
      cast(bill_id as integer) as cost_event_id,
      'Drug' as cost_domain_id,
      32869 as cost_type_concept_id,
      44818668 as currency_concept_id,
      cast(total_charge_per_bill as float) as total_charge,
      cast(null as float) as total_cost,
      cast(total_amount_paid_per_bill as float) as total_paid,
      cast(null as float) as paid_by_payer,
      cast(null as float) as paid_by_patient,
      cast(null as float) as paid_patient_copay,
      cast(null as float) as paid_patient_coinsurance,
      cast(null as float) as paid_patient_deductible,
      cast(null as float) as paid_by_primary,
      cast(null as float) as paid_ingredient_cost,
      cast(null as float) as paid_dispensing_fee,
      cast(null as integer) as payer_plan_period_id,
      cast(null as float) as amount_allowed,
      cast(null as integer) as revenue_code_concept_id,
      cast(null as varchar) as revenue_code_source_value,
      cast(null as integer) as drg_concept_id,
      cast(null as varchar) as drg_source_value
  from {{ source('raw', 'pharmacy_header_historical') }}
)
  {% endset %}
  {% do cte_definitions.append(query) %}
{% endif %}

{% if has_current or has_historical %}
with
  {{ cte_definitions | join(",\n") }}

select *
from (
  {% if has_current %}
    select * from institutional_header_current
    union
    select * from professional_header_current
    union
    select * from pharmacy_header_current
  {% endif %}
  {% if has_current and has_historical %}
    union
  {% endif %}
  {% if has_historical %}
    select * from institutional_header_historical
    union
    select * from professional_header_historical
    union
    select * from pharmacy_header_historical
  {% endif %}
) as final_result
{% else %}
-- No source tables available - return empty result set with OMOP cost schema
select
    cast(null as varchar) as cost_id,
    cast(null as integer) as cost_event_id,
    cast(null as varchar) as cost_domain_id,
    cast(null as integer) as cost_type_concept_id,
    cast(null as integer) as currency_concept_id,
    cast(null as float) as total_charge,
    cast(null as float) as total_cost,
    cast(null as float) as total_paid,
    cast(null as float) as paid_by_payer,
    cast(null as float) as paid_by_patient,
    cast(null as float) as paid_patient_copay,
    cast(null as float) as paid_patient_coinsurance,
    cast(null as float) as paid_patient_deductible,
    cast(null as float) as paid_by_primary,
    cast(null as float) as paid_ingredient_cost,
    cast(null as float) as paid_dispensing_fee,
    cast(null as integer) as payer_plan_period_id,
    cast(null as float) as amount_allowed,
    cast(null as integer) as revenue_code_concept_id,
    cast(null as varchar) as revenue_code_source_value,
    cast(null as integer) as drg_concept_id,
    cast(null as varchar) as drg_source_value
where false
{% endif %}
