{% set has_current = check_table_exists('raw', 'institutional_header_current') %}
{% set has_historical = check_table_exists('raw', 'institutional_header_historical') %}

{% set has_inst_detail_current = check_table_exists('raw', 'institutional_detail_current') %}
{% set has_prof_detail_current = check_table_exists('raw', 'professional_detail_current') %}
{% set has_pharm_detail_current = check_table_exists('raw', 'pharmacy_detail_current') %}
{% set has_inst_detail_historical = check_table_exists('raw', 'institutional_detail_historical') %}
{% set has_prof_detail_historical = check_table_exists('raw', 'professional_detail_historical') %}
{% set has_pharm_detail_historical = check_table_exists('raw', 'pharmacy_detail_historical') %}

{% set cte_definitions = [] %}
{% set union_queries = [] %}

{# ===== BILL-LEVEL COSTS (header tables only) ===== #}

{% if has_current %}
  {% set query %}
-- Bill-level costs from institutional header (no detail join to avoid duplication)
institutional_bill_cost_current as (
  select
      cast(
        hash(concat_ws('||', 'bill', bill_id, total_charge_per_bill), 'xxhash64') % 1000000000 as varchar
      ) as cost_id,
      cast(bill_id as integer) as cost_event_id,
      'Visit' as cost_domain_id,
      32855 as cost_type_concept_id,
      44818668 as currency_concept_id,
      TRY_CAST(total_charge_per_bill as float) as total_charge,
      cast(null as float) as total_cost,
      TRY_CAST(total_amount_paid_per_bill as float) as total_paid,
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
      diagnosis_related_group_code as drg_source_value
  from {{ source('raw', 'institutional_header_current') }}
),
professional_bill_cost_current as (
  select
      cast(
        hash(concat_ws('||', 'bill', bill_id, total_charge_per_bill), 'xxhash64') % 1000000000 as varchar
      ) as cost_id,
      cast(bill_id as integer) as cost_event_id,
      'Visit' as cost_domain_id,
      32873 as cost_type_concept_id,
      44818668 as currency_concept_id,
      TRY_CAST(total_charge_per_bill as float) as total_charge,
      cast(null as float) as total_cost,
      TRY_CAST(total_amount_paid_per_bill as float) as total_paid,
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
pharmacy_bill_cost_current as (
  select
      cast(
        hash(concat_ws('||', 'bill', bill_id, total_charge_per_bill), 'xxhash64') % 1000000000 as varchar
      ) as cost_id,
      cast(bill_id as integer) as cost_event_id,
      'Visit' as cost_domain_id,
      32869 as cost_type_concept_id,
      44818668 as currency_concept_id,
      TRY_CAST(total_charge_per_bill as float) as total_charge,
      cast(null as float) as total_cost,
      TRY_CAST(total_amount_paid_per_bill as float) as total_paid,
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
  {% do union_queries.append('select * from institutional_bill_cost_current') %}
  {% do union_queries.append('select * from professional_bill_cost_current') %}
  {% do union_queries.append('select * from pharmacy_bill_cost_current') %}
{% endif %}

{# ===== LINE-LEVEL COSTS (detail tables) ===== #}

{% if has_inst_detail_current %}
  {% set query %}
-- Line-level costs from institutional detail
institutional_line_cost_current as (
  select
      cast(
        hash(concat_ws('||', 'line', bill_id, row_id), 'xxhash64') % 1000000000 as varchar
      ) as cost_id,
      -- Link to visit_detail_id (same hash used in stg_visit_detail)
      cast(
        hash(concat_ws('||', bill_id, row_id), 'xxhash64') % 1000000000 as integer
      ) as cost_event_id,
      'Visit Detail' as cost_domain_id,
      32855 as cost_type_concept_id,
      44818668 as currency_concept_id,
      TRY_CAST(total_charge_per_line as float) as total_charge,
      cast(null as float) as total_cost,
      TRY_CAST(total_amount_paid_per_line as float) as total_paid,
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
      revenue_billed_code as revenue_code_source_value,
      cast(null as integer) as drg_concept_id,
      cast(null as varchar) as drg_source_value
  from {{ source('raw', 'institutional_detail_current') }}
  where total_charge_per_line is not null or total_amount_paid_per_line is not null
)
  {% endset %}
  {% do cte_definitions.append(query) %}
  {% do union_queries.append('select * from institutional_line_cost_current') %}
{% endif %}

{% if has_prof_detail_current %}
  {% set query %}
-- Line-level costs from professional detail
professional_line_cost_current as (
  select
      cast(
        hash(concat_ws('||', 'line', bill_id, row_id), 'xxhash64') % 1000000000 as varchar
      ) as cost_id,
      cast(
        hash(concat_ws('||', bill_id, row_id), 'xxhash64') % 1000000000 as integer
      ) as cost_event_id,
      'Visit Detail' as cost_domain_id,
      32873 as cost_type_concept_id,
      44818668 as currency_concept_id,
      TRY_CAST(total_charge_per_line as float) as total_charge,
      cast(null as float) as total_cost,
      TRY_CAST(total_amount_paid_per_line as float) as total_paid,
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
  from {{ source('raw', 'professional_detail_current') }}
  where total_charge_per_line is not null or total_amount_paid_per_line is not null
)
  {% endset %}
  {% do cte_definitions.append(query) %}
  {% do union_queries.append('select * from professional_line_cost_current') %}
{% endif %}

{% if has_pharm_detail_current %}
  {% set query %}
-- Line-level costs from pharmacy detail with dispensing fee
pharmacy_line_cost_current as (
  select
      cast(
        hash(concat_ws('||', 'line', bill_id, row_id), 'xxhash64') % 1000000000 as varchar
      ) as cost_id,
      cast(
        hash(concat_ws('||', bill_id, row_id), 'xxhash64') % 1000000000 as integer
      ) as cost_event_id,
      'Visit Detail' as cost_domain_id,
      32869 as cost_type_concept_id,
      44818668 as currency_concept_id,
      TRY_CAST(drugs_supplies_billed_amount as float) as total_charge,
      cast(null as float) as total_cost,
      TRY_CAST(total_amount_paid_per_line as float) as total_paid,
      cast(null as float) as paid_by_payer,
      cast(null as float) as paid_by_patient,
      cast(null as float) as paid_patient_copay,
      cast(null as float) as paid_patient_coinsurance,
      cast(null as float) as paid_patient_deductible,
      cast(null as float) as paid_by_primary,
      cast(null as float) as paid_ingredient_cost,
      TRY_CAST(drugs_supplies_dispensing as float) as paid_dispensing_fee,
      cast(null as integer) as payer_plan_period_id,
      cast(null as float) as amount_allowed,
      cast(null as integer) as revenue_code_concept_id,
      cast(null as varchar) as revenue_code_source_value,
      cast(null as integer) as drg_concept_id,
      cast(null as varchar) as drg_source_value
  from {{ source('raw', 'pharmacy_detail_current') }}
  where drugs_supplies_billed_amount is not null or total_amount_paid_per_line is not null
)
  {% endset %}
  {% do cte_definitions.append(query) %}
  {% do union_queries.append('select * from pharmacy_line_cost_current') %}
{% endif %}

{# ===== HISTORICAL ===== #}

{% if has_historical %}
  {% set query %}
institutional_bill_cost_historical as (
  select
      cast(
        hash(concat_ws('||', 'bill', bill_id, total_charge_per_bill), 'xxhash64') % 1000000000 as varchar
      ) as cost_id,
      cast(bill_id as integer) as cost_event_id,
      'Visit' as cost_domain_id,
      32855 as cost_type_concept_id,
      44818668 as currency_concept_id,
      TRY_CAST(total_charge_per_bill as float) as total_charge,
      cast(null as float) as total_cost,
      TRY_CAST(total_amount_paid_per_bill as float) as total_paid,
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
      diagnosis_related_group_code as drg_source_value
  from {{ source('raw', 'institutional_header_historical') }}
),
professional_bill_cost_historical as (
  select
      cast(
        hash(concat_ws('||', 'bill', bill_id, total_charge_per_bill), 'xxhash64') % 1000000000 as varchar
      ) as cost_id,
      cast(bill_id as integer) as cost_event_id,
      'Visit' as cost_domain_id,
      32873 as cost_type_concept_id,
      44818668 as currency_concept_id,
      TRY_CAST(total_charge_per_bill as float) as total_charge,
      cast(null as float) as total_cost,
      TRY_CAST(total_amount_paid_per_bill as float) as total_paid,
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
pharmacy_bill_cost_historical as (
  select
      cast(
        hash(concat_ws('||', 'bill', bill_id, total_charge_per_bill), 'xxhash64') % 1000000000 as varchar
      ) as cost_id,
      cast(bill_id as integer) as cost_event_id,
      'Visit' as cost_domain_id,
      32869 as cost_type_concept_id,
      44818668 as currency_concept_id,
      TRY_CAST(total_charge_per_bill as float) as total_charge,
      cast(null as float) as total_cost,
      TRY_CAST(total_amount_paid_per_bill as float) as total_paid,
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
  {% do union_queries.append('select * from institutional_bill_cost_historical') %}
  {% do union_queries.append('select * from professional_bill_cost_historical') %}
  {% do union_queries.append('select * from pharmacy_bill_cost_historical') %}
{% endif %}

{% if has_inst_detail_historical %}
  {% set query %}
institutional_line_cost_historical as (
  select
      cast(
        hash(concat_ws('||', 'line', bill_id, row_id), 'xxhash64') % 1000000000 as varchar
      ) as cost_id,
      cast(
        hash(concat_ws('||', bill_id, row_id), 'xxhash64') % 1000000000 as integer
      ) as cost_event_id,
      'Visit Detail' as cost_domain_id,
      32855 as cost_type_concept_id,
      44818668 as currency_concept_id,
      TRY_CAST(total_charge_per_line as float) as total_charge,
      cast(null as float) as total_cost,
      TRY_CAST(total_amount_paid_per_line as float) as total_paid,
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
      revenue_billed_code as revenue_code_source_value,
      cast(null as integer) as drg_concept_id,
      cast(null as varchar) as drg_source_value
  from {{ source('raw', 'institutional_detail_historical') }}
  where total_charge_per_line is not null or total_amount_paid_per_line is not null
)
  {% endset %}
  {% do cte_definitions.append(query) %}
  {% do union_queries.append('select * from institutional_line_cost_historical') %}
{% endif %}

{% if has_prof_detail_historical %}
  {% set query %}
professional_line_cost_historical as (
  select
      cast(
        hash(concat_ws('||', 'line', bill_id, row_id), 'xxhash64') % 1000000000 as varchar
      ) as cost_id,
      cast(
        hash(concat_ws('||', bill_id, row_id), 'xxhash64') % 1000000000 as integer
      ) as cost_event_id,
      'Visit Detail' as cost_domain_id,
      32873 as cost_type_concept_id,
      44818668 as currency_concept_id,
      TRY_CAST(total_charge_per_line as float) as total_charge,
      cast(null as float) as total_cost,
      TRY_CAST(total_amount_paid_per_line as float) as total_paid,
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
  from {{ source('raw', 'professional_detail_historical') }}
  where total_charge_per_line is not null or total_amount_paid_per_line is not null
)
  {% endset %}
  {% do cte_definitions.append(query) %}
  {% do union_queries.append('select * from professional_line_cost_historical') %}
{% endif %}

{% if has_pharm_detail_historical %}
  {% set query %}
pharmacy_line_cost_historical as (
  select
      cast(
        hash(concat_ws('||', 'line', bill_id, row_id), 'xxhash64') % 1000000000 as varchar
      ) as cost_id,
      cast(
        hash(concat_ws('||', bill_id, row_id), 'xxhash64') % 1000000000 as integer
      ) as cost_event_id,
      'Visit Detail' as cost_domain_id,
      32869 as cost_type_concept_id,
      44818668 as currency_concept_id,
      TRY_CAST(drugs_supplies_billed_amount as float) as total_charge,
      cast(null as float) as total_cost,
      TRY_CAST(total_amount_paid_per_line as float) as total_paid,
      cast(null as float) as paid_by_payer,
      cast(null as float) as paid_by_patient,
      cast(null as float) as paid_patient_copay,
      cast(null as float) as paid_patient_coinsurance,
      cast(null as float) as paid_patient_deductible,
      cast(null as float) as paid_by_primary,
      cast(null as float) as paid_ingredient_cost,
      TRY_CAST(drugs_supplies_dispensing as float) as paid_dispensing_fee,
      cast(null as integer) as payer_plan_period_id,
      cast(null as float) as amount_allowed,
      cast(null as integer) as revenue_code_concept_id,
      cast(null as varchar) as revenue_code_source_value,
      cast(null as integer) as drg_concept_id,
      cast(null as varchar) as drg_source_value
  from {{ source('raw', 'pharmacy_detail_historical') }}
  where drugs_supplies_billed_amount is not null or total_amount_paid_per_line is not null
)
  {% endset %}
  {% do cte_definitions.append(query) %}
  {% do union_queries.append('select * from pharmacy_line_cost_historical') %}
{% endif %}

{% if union_queries | length > 0 %}
with
  {{ cte_definitions | join(",\n") }}

select *
from (
  {{ union_queries | join("\n  union all\n  ") }}
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
