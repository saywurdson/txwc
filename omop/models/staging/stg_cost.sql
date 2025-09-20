{% set exists_i_current = check_table_exists('raw', 'institutional_header_current') %}
{% set exists_id_current = check_table_exists('raw', 'institutional_detail_current') %}
{% set exists_i_historical = check_table_exists('raw', 'institutional_header_historical') %}
{% set exists_id_historical = check_table_exists('raw', 'institutional_detail_historical') %}
{% set exists_pr_current = check_table_exists('raw', 'professional_header_current') %}
{% set exists_prd_current = check_table_exists('raw', 'professional_detail_current') %}
{% set exists_pr_historical = check_table_exists('raw', 'professional_header_historical') %}
{% set exists_prd_historical = check_table_exists('raw', 'professional_detail_historical') %}
{% set exists_ph_current = check_table_exists('raw', 'pharmacy_header_current') %}
{% set exists_phd_current = check_table_exists('raw', 'pharmacy_detail_current') %}
{% set exists_ph_historical = check_table_exists('raw', 'pharmacy_header_historical') %}
{% set exists_phd_historical = check_table_exists('raw', 'pharmacy_detail_historical') %}

{% set cte_definitions = [] %}

{% if exists_i_current and exists_id_current %}
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
      cast(null as integer) as cost_domain_id,
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
)
  {% endset %}
  {% do cte_definitions.append(query) %}
{% endif %}

{% if exists_i_historical and exists_id_historical %}
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
      cast(null as integer) as cost_domain_id,
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
)
  {% endset %}
  {% do cte_definitions.append(query) %}
{% endif %}

{% if exists_pr_current %}
  {% set query %}
professional_header_current as (
  select distinct
      cast(
        hash(concat_ws('||', 
          row_id, 
          bill_id,
          total_charge_per_bill), 'xxhash64') % 1000000000 as varchar
      ) as cost_id,
      cast(bill_id as integer) as cost_event_id,
      cast(null as integer) as cost_domain_id,
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
)
  {% endset %}
  {% do cte_definitions.append(query) %}
{% endif %}

{% if exists_pr_historical %}
  {% set query %}
professional_header_historical as (
  select distinct
      cast(
        hash(concat_ws('||', 
          row_id, 
          bill_id,
          total_charge_per_bill), 'xxhash64') % 1000000000 as varchar
      ) as cost_id,
      cast(bill_id as integer) as cost_event_id,
      cast(null as integer) as cost_domain_id,
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
)
  {% endset %}
  {% do cte_definitions.append(query) %}
{% endif %}

{% if exists_ph_current %}
  {% set query %}
pharmacy_header_current as (
  select distinct
      cast(
        hash(concat_ws('||', 
          row_id, 
          bill_id,
          total_charge_per_bill), 'xxhash64') % 1000000000 as varchar
      ) as cost_id,
      cast(bill_id as integer) as cost_event_id,
      cast(null as integer) as cost_domain_id,
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

{% if exists_ph_historical %}
  {% set query %}
pharmacy_header_historical as (
  select distinct
      cast(
        hash(concat_ws('||', 
          row_id, 
          bill_id,
          total_charge_per_bill), 'xxhash64') % 1000000000 as varchar
      ) as cost_id,
      cast(bill_id as integer) as cost_event_id,
      cast(null as integer) as cost_domain_id,
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

{% set union_queries = [] %}
{% if exists_i_current %}
  {% do union_queries.append('select * from institutional_header_current') %}
{% endif %}
{% if exists_i_historical %}
  {% do union_queries.append('select * from institutional_header_historical') %}
{% endif %}
{% if exists_pr_current %}
  {% do union_queries.append('select * from professional_header_current') %}
{% endif %}
{% if exists_pr_historical %}
  {% do union_queries.append('select * from professional_header_historical') %}
{% endif %}
{% if exists_ph_current %}
  {% do union_queries.append('select * from pharmacy_header_current') %}
{% endif %}
{% if exists_ph_historical %}
  {% do union_queries.append('select * from pharmacy_header_historical') %}
{% endif %}

{% if cte_definitions | length > 0 %}
with
  {{ cte_definitions | join(",\n") }}
{% endif %}
select *
from (
  {{ union_queries | join(" union ") }}
) as final_result