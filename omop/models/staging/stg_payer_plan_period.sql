{% set has_current = check_table_exists('raw', 'institutional_header_current') %}
{% set has_historical = check_table_exists('raw', 'institutional_header_historical') %}

{% set cte_definitions = [] %}
{% set union_queries = [] %}

{% if has_current %}
  {% set query %}
institutional_payer_current as (
  select
      {{ derive_person_id() }} as person_id,
      try_cast(reporting_period_start_date as date) as payer_plan_period_start_date,
      try_cast(reporting_period_end_date as date) as payer_plan_period_end_date,
      cast(insurer_fein as varchar) as payer_source_value,
      cast(claim_administrator_name as varchar) as plan_source_value,
      cast(claim_administrator_fein as varchar) as sponsor_source_value,
      cast(claim_administrator_claim as varchar) as family_source_value,
      cast(contract_type_code as varchar) as stop_reason_source_value
  from {{ source('raw', 'institutional_header_current') }}
  where insurer_fein is not null or claim_administrator_fein is not null
),
professional_payer_current as (
  select
      {{ derive_person_id() }} as person_id,
      try_cast(reporting_period_start_date as date) as payer_plan_period_start_date,
      try_cast(reporting_period_end_date as date) as payer_plan_period_end_date,
      cast(insurer_fein as varchar) as payer_source_value,
      cast(claim_administrator_name as varchar) as plan_source_value,
      cast(claim_administrator_fein as varchar) as sponsor_source_value,
      cast(claim_administrator_claim as varchar) as family_source_value,
      cast(null as varchar) as stop_reason_source_value
  from {{ source('raw', 'professional_header_current') }}
  where insurer_fein is not null or claim_administrator_fein is not null
),
pharmacy_payer_current as (
  select
      {{ derive_person_id() }} as person_id,
      try_cast(reporting_period_start_date as date) as payer_plan_period_start_date,
      try_cast(reporting_period_end_date as date) as payer_plan_period_end_date,
      cast(insurer_fein as varchar) as payer_source_value,
      cast(claim_administrator_name as varchar) as plan_source_value,
      cast(claim_administrator_fein as varchar) as sponsor_source_value,
      cast(claim_administrator_claim as varchar) as family_source_value,
      cast(null as varchar) as stop_reason_source_value
  from {{ source('raw', 'pharmacy_header_current') }}
  where insurer_fein is not null or claim_administrator_fein is not null
)
  {% endset %}
  {% do cte_definitions.append(query) %}
  {% do union_queries.append('select * from institutional_payer_current') %}
  {% do union_queries.append('select * from professional_payer_current') %}
  {% do union_queries.append('select * from pharmacy_payer_current') %}
{% endif %}

{% if has_historical %}
  {% set query %}
institutional_payer_historical as (
  select
      {{ derive_person_id() }} as person_id,
      try_cast(reporting_period_start_date as date) as payer_plan_period_start_date,
      try_cast(reporting_period_end_date as date) as payer_plan_period_end_date,
      cast(insurer_fein as varchar) as payer_source_value,
      cast(claim_administrator_name as varchar) as plan_source_value,
      cast(claim_administrator_fein as varchar) as sponsor_source_value,
      cast(claim_administrator_claim as varchar) as family_source_value,
      cast(contract_type_code as varchar) as stop_reason_source_value
  from {{ source('raw', 'institutional_header_historical') }}
  where insurer_fein is not null or claim_administrator_fein is not null
),
professional_payer_historical as (
  select
      {{ derive_person_id() }} as person_id,
      try_cast(reporting_period_start_date as date) as payer_plan_period_start_date,
      try_cast(reporting_period_end_date as date) as payer_plan_period_end_date,
      cast(insurer_fein as varchar) as payer_source_value,
      cast(claim_administrator_name as varchar) as plan_source_value,
      cast(claim_administrator_fein as varchar) as sponsor_source_value,
      cast(claim_administrator_claim as varchar) as family_source_value,
      cast(null as varchar) as stop_reason_source_value
  from {{ source('raw', 'professional_header_historical') }}
  where insurer_fein is not null or claim_administrator_fein is not null
),
pharmacy_payer_historical as (
  select
      {{ derive_person_id() }} as person_id,
      try_cast(reporting_period_start_date as date) as payer_plan_period_start_date,
      try_cast(reporting_period_end_date as date) as payer_plan_period_end_date,
      cast(insurer_fein as varchar) as payer_source_value,
      cast(claim_administrator_name as varchar) as plan_source_value,
      cast(claim_administrator_fein as varchar) as sponsor_source_value,
      cast(claim_administrator_claim as varchar) as family_source_value,
      cast(null as varchar) as stop_reason_source_value
  from {{ source('raw', 'pharmacy_header_historical') }}
  where insurer_fein is not null or claim_administrator_fein is not null
)
  {% endset %}
  {% do cte_definitions.append(query) %}
  {% do union_queries.append('select * from institutional_payer_historical') %}
  {% do union_queries.append('select * from professional_payer_historical') %}
  {% do union_queries.append('select * from pharmacy_payer_historical') %}
{% endif %}

{% if union_queries | length > 0 %}
with
  {{ cte_definitions | join(",\n") }}

select *
from (
  {{ union_queries | join("\n  union all\n  ") }}
) as final_result
{% else %}
select
    cast(null as integer) as person_id,
    cast(null as date) as payer_plan_period_start_date,
    cast(null as date) as payer_plan_period_end_date,
    cast(null as varchar) as payer_source_value,
    cast(null as varchar) as plan_source_value,
    cast(null as varchar) as sponsor_source_value,
    cast(null as varchar) as family_source_value,
    cast(null as varchar) as stop_reason_source_value
where false
{% endif %}
