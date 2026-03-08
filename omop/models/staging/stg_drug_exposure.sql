-- Check if current or historical data exists (use institutional_header as representative)
{% set has_current = check_table_exists('raw', 'institutional_header_current') %}
{% set has_historical = check_table_exists('raw', 'institutional_header_historical') %}

{% set drug_type_mapping = {
    'pharmacy': 32869,
    'institutional': 32854,
    'professional': 32854
} %}

{% set cte_queries = [] %}

{% if has_current %}
  -- Pharmacy detail current
  {% set query %}
  pharmacy_detail_current as (
    select
      cast(hash(concat_ws('||', pdc.bill_id, pdc.row_id), 'xxhash64') % 1000000000 as varchar) as drug_exposure_id,
      {{ derive_person_id('phc') }} as person_id,
      cast(null as integer) as drug_concept_id,
      coalesce(try_cast(pdc.service_line_from_date as date), try_cast(pdc.prescription_line_date as date)) as drug_exposure_start_date,
      coalesce(try_cast(pdc.service_line_from_date as timestamp), try_cast(pdc.prescription_line_date as timestamp)) as drug_exposure_start_datetime,
      -- Calculate end date: start_date + days_supply, fallback to visit_end_date
      COALESCE(
          coalesce(try_cast(pdc.service_line_from_date as date), try_cast(pdc.prescription_line_date as date)) + try_cast(pdc.drugs_supplies_number_of as integer),
          try_cast(phc.reporting_period_end_date as date)
      ) as drug_exposure_end_date,
      COALESCE(
          coalesce(try_cast(pdc.service_line_from_date as timestamp), try_cast(pdc.prescription_line_date as timestamp)) + (try_cast(pdc.drugs_supplies_number_of as integer) * INTERVAL '1' DAY),
          try_cast(phc.reporting_period_end_date as timestamp)
      ) as drug_exposure_end_datetime,
      cast(null as date) as verbatim_end_date,
      {{ drug_type_mapping['pharmacy'] }} as drug_type_concept_id,
      cast(null as varchar) as stop_reason,
      0 as refills,
      try_cast(pdc.drugs_supplies_quantity as float) as quantity,
      try_cast(pdc.drugs_supplies_number_of as integer) as days_supply,
      cast(null as varchar) as sig,
      cast(null as integer) as route_concept_id,
      cast(null as integer) as lot_number,
      cast(hash(concat_ws('||',
        phc.rendering_bill_provider_last,
        coalesce(phc.rendering_bill_provider_first, ''),
        phc.rendering_bill_provider_state_1,
        phc.rendering_bill_provider_4
      ), 'xxhash64') % 1000000000 as varchar) as provider_id,
      cast(pdc.bill_id as varchar) as visit_occurrence_id,
      -- visit_detail_id uses same hash as visit_detail table
      cast(hash(concat_ws('||', pdc.bill_id, pdc.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
      pdc.ndc_billed_code as drug_source_value,
      cast(null as integer) as drug_source_concept_id,
      cast(null as varchar) as route_source_value,
      cast(null as varchar) as dose_unit_source_value
    from {{ source('raw', 'pharmacy_detail_current') }} pdc
    join {{ source('raw', 'pharmacy_header_current') }} phc
      on cast(pdc.bill_id as varchar) = cast(phc.bill_id as varchar)
  )
  {% endset %}
  {% do cte_queries.append(query) %}

  -- Institutional detail current
  {% set query %}
  institutional_detail_current as (
    select
      cast(hash(concat_ws('||', idc.bill_id, idc.row_id), 'xxhash64') % 1000000000 as varchar) as drug_exposure_id,
      {{ derive_person_id('ihc') }} as person_id,
      cast(null as integer) as drug_concept_id,
      CASE WHEN idc.service_line_from_date = 'N' THEN NULL
          ELSE try_cast(idc.service_line_from_date as date) END as drug_exposure_start_date,
      CASE WHEN idc.service_line_from_date = 'N' THEN NULL
          ELSE try_cast(idc.service_line_from_date as timestamp) END as drug_exposure_start_datetime,
      -- Use GREATEST of: service_line_to_date vs (start_date + days_supply), then fallback to visit_end_date
      COALESCE(
          GREATEST(
              CASE WHEN idc.service_line_to_date = 'N' THEN NULL ELSE try_cast(idc.service_line_to_date as date) END,
              CASE WHEN idc.service_line_from_date != 'N' AND idc.service_line_from_date IS NOT NULL
                   AND try_cast(idc.days_units_billed as integer) IS NOT NULL
                   THEN try_cast(idc.service_line_from_date as date) + try_cast(idc.days_units_billed as integer)
                   ELSE NULL END
          ),
          try_cast(ihc.reporting_period_end_date as date)
      ) as drug_exposure_end_date,
      COALESCE(
          GREATEST(
              CASE WHEN idc.service_line_to_date = 'N' THEN NULL ELSE try_cast(idc.service_line_to_date as timestamp) END,
              CASE WHEN idc.service_line_from_date != 'N' AND idc.service_line_from_date IS NOT NULL
                   AND try_cast(idc.days_units_billed as integer) IS NOT NULL
                   THEN try_cast(idc.service_line_from_date as timestamp) + (try_cast(idc.days_units_billed as integer) * INTERVAL '1' DAY)
                   ELSE NULL END
          ),
          try_cast(ihc.reporting_period_end_date as timestamp)
      ) as drug_exposure_end_datetime,
      CASE WHEN idc.service_line_to_date = 'N' THEN NULL
          ELSE try_cast(idc.service_line_to_date as date) END as verbatim_end_date,
      {{ drug_type_mapping['institutional'] }} as drug_type_concept_id,
      cast(null as varchar) as stop_reason,
      0 as refills,
      cast(null as integer) as quantity,
      try_cast(idc.days_units_billed as integer) as days_supply,
      cast(null as varchar) as sig,
      cast(null as integer) as route_concept_id,
      cast(null as integer) as lot_number,
      cast(hash(concat_ws('||',
        ihc.rendering_bill_provider_last,
        coalesce(ihc.rendering_bill_provider_first, ''),
        ihc.rendering_bill_provider_state_1,
        ihc.rendering_bill_provider_4
      ), 'xxhash64') % 1000000000 as varchar) as provider_id,
      cast(idc.bill_id as varchar) as visit_occurrence_id,
      -- visit_detail_id uses same hash as visit_detail table
      cast(hash(concat_ws('||', idc.bill_id, idc.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
      idc.hcpcs_line_procedure_billed as drug_source_value,
      cast(null as integer) as drug_source_concept_id,
      cast(null as varchar) as route_source_value,
      cast(null as varchar) as dose_unit_source_value
    from {{ source('raw', 'institutional_detail_current') }} idc
    join {{ source('raw', 'institutional_header_current') }} ihc
      on cast(idc.bill_id as varchar) = cast(ihc.bill_id as varchar)
    join {{ source('omop', 'concept') }} as c
      on c.concept_code = idc.hcpcs_line_procedure_billed
    where c.domain_id = 'Drug'
      and c.vocabulary_id = 'HCPCS'
  )
  {% endset %}
  {% do cte_queries.append(query) %}

  -- Professional detail current
  {% set query %}
  professional_detail_current as (
    select
      cast(hash(concat_ws('||', prdc.bill_id, prdc.row_id), 'xxhash64') % 1000000000 as varchar) as drug_exposure_id,
      {{ derive_person_id('prhc') }} as person_id,
      cast(null as integer) as drug_concept_id,
      try_cast(prdc.service_line_from_date as date) as drug_exposure_start_date,
      try_cast(prdc.service_line_from_date as timestamp) as drug_exposure_start_datetime,
      -- Use GREATEST of: service_line_to_date vs (start_date + days_supply), then fallback to visit_end_date
      COALESCE(
          GREATEST(
              try_cast(prdc.service_line_to_date as date),
              CASE WHEN try_cast(prdc.service_line_from_date as date) IS NOT NULL
                   AND try_cast(prdc.days_units_billed as integer) IS NOT NULL
                   THEN try_cast(prdc.service_line_from_date as date) + try_cast(prdc.days_units_billed as integer)
                   ELSE NULL END
          ),
          try_cast(prhc.reporting_period_end_date as date)
      ) as drug_exposure_end_date,
      COALESCE(
          GREATEST(
              try_cast(prdc.service_line_to_date as timestamp),
              CASE WHEN try_cast(prdc.service_line_from_date as timestamp) IS NOT NULL
                   AND try_cast(prdc.days_units_billed as integer) IS NOT NULL
                   THEN try_cast(prdc.service_line_from_date as timestamp) + (try_cast(prdc.days_units_billed as integer) * INTERVAL '1' DAY)
                   ELSE NULL END
          ),
          try_cast(prhc.reporting_period_end_date as timestamp)
      ) as drug_exposure_end_datetime,
      try_cast(prdc.service_line_to_date as date) as verbatim_end_date,
      {{ drug_type_mapping['professional'] }} as drug_type_concept_id,
      cast(null as varchar) as stop_reason,
      0 as refills,
      cast(null as integer) as quantity,
      try_cast(prdc.days_units_billed as integer) as days_supply,
      cast(null as varchar) as sig,
      cast(null as integer) as route_concept_id,
      cast(null as integer) as lot_number,
      cast(hash(concat_ws('||',
        prhc.rendering_bill_provider_last,
        coalesce(prhc.rendering_bill_provider_first, ''),
        prhc.rendering_bill_provider_state_1,
        prhc.rendering_bill_provider_4
      ), 'xxhash64') % 1000000000 as varchar) as provider_id,
      cast(prdc.bill_id as varchar) as visit_occurrence_id,
      -- visit_detail_id uses same hash as visit_detail table
      cast(hash(concat_ws('||', prdc.bill_id, prdc.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
      prdc.hcpcs_line_procedure_billed as drug_source_value,
      cast(null as integer) as drug_source_concept_id,
      cast(null as varchar) as route_source_value,
      cast(null as varchar) as dose_unit_source_value
    from {{ source('raw', 'professional_detail_current') }} prdc
    join {{ source('raw', 'professional_header_current') }} prhc
      on cast(prdc.bill_id as varchar) = cast(prhc.bill_id as varchar)
    join {{ source('omop', 'concept') }} as c
      on c.concept_code = prdc.hcpcs_line_procedure_billed
    where c.domain_id = 'Drug'
      and c.vocabulary_id = 'HCPCS'
  )
  {% endset %}
  {% do cte_queries.append(query) %}
{% endif %}

{% if has_historical %}
  -- Pharmacy detail historical
  {% set query %}
  pharmacy_detail_historical as (
    select
      cast(hash(concat_ws('||', pdc.bill_id, pdc.row_id), 'xxhash64') % 1000000000 as varchar) as drug_exposure_id,
      {{ derive_person_id('phc') }} as person_id,
      cast(null as integer) as drug_concept_id,
      coalesce(try_cast(pdc.service_line_from_date as date), try_cast(pdc.prescription_line_date as date)) as drug_exposure_start_date,
      coalesce(try_cast(pdc.service_line_from_date as timestamp), try_cast(pdc.prescription_line_date as timestamp)) as drug_exposure_start_datetime,
      -- Calculate end date: start_date + days_supply, fallback to visit_end_date
      COALESCE(
          coalesce(try_cast(pdc.service_line_from_date as date), try_cast(pdc.prescription_line_date as date)) + try_cast(pdc.drugs_supplies_number_of as integer),
          try_cast(phc.reporting_period_end_date as date)
      ) as drug_exposure_end_date,
      COALESCE(
          coalesce(try_cast(pdc.service_line_from_date as timestamp), try_cast(pdc.prescription_line_date as timestamp)) + (try_cast(pdc.drugs_supplies_number_of as integer) * INTERVAL '1' DAY),
          try_cast(phc.reporting_period_end_date as timestamp)
      ) as drug_exposure_end_datetime,
      cast(null as date) as verbatim_end_date,
      {{ drug_type_mapping['pharmacy'] }} as drug_type_concept_id,
      cast(null as varchar) as stop_reason,
      0 as refills,
      try_cast(pdc.drugs_supplies_quantity as float) as quantity,
      try_cast(pdc.drugs_supplies_number_of as integer) as days_supply,
      cast(null as varchar) as sig,
      cast(null as integer) as route_concept_id,
      cast(null as integer) as lot_number,
      cast(hash(concat_ws('||',
        phc.rendering_bill_provider_last,
        coalesce(phc.rendering_bill_provider_first, ''),
        phc.rendering_bill_provider_state_1,
        phc.rendering_bill_provider_4
      ), 'xxhash64') % 1000000000 as varchar) as provider_id,
      cast(pdc.bill_id as varchar) as visit_occurrence_id,
      -- visit_detail_id uses same hash as visit_detail table
      cast(hash(concat_ws('||', pdc.bill_id, pdc.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
      pdc.ndc_billed_code as drug_source_value,
      cast(null as integer) as drug_source_concept_id,
      cast(null as varchar) as route_source_value,
      cast(null as varchar) as dose_unit_source_value
    from {{ source('raw', 'pharmacy_detail_historical') }} pdc
    join {{ source('raw', 'pharmacy_header_historical') }} phc
      on cast(pdc.bill_id as varchar) = cast(phc.bill_id as varchar)
  )
  {% endset %}
  {% do cte_queries.append(query) %}

  -- Institutional detail historical
  {% set query %}
  institutional_detail_historical as (
    select
      cast(hash(concat_ws('||', idc.bill_id, idc.row_id), 'xxhash64') % 1000000000 as varchar) as drug_exposure_id,
      {{ derive_person_id('ihc') }} as person_id,
      cast(null as integer) as drug_concept_id,
      CASE WHEN idc.service_line_from_date = 'N' THEN NULL
          ELSE try_cast(idc.service_line_from_date as date) END as drug_exposure_start_date,
      CASE WHEN idc.service_line_from_date = 'N' THEN NULL
          ELSE try_cast(idc.service_line_from_date as timestamp) END as drug_exposure_start_datetime,
      -- Use GREATEST of: service_line_to_date vs (start_date + days_supply), then fallback to visit_end_date
      COALESCE(
          GREATEST(
              CASE WHEN idc.service_line_to_date = 'N' THEN NULL ELSE try_cast(idc.service_line_to_date as date) END,
              CASE WHEN idc.service_line_from_date != 'N' AND idc.service_line_from_date IS NOT NULL
                   AND try_cast(idc.days_units_billed as integer) IS NOT NULL
                   THEN try_cast(idc.service_line_from_date as date) + try_cast(idc.days_units_billed as integer)
                   ELSE NULL END
          ),
          try_cast(ihc.reporting_period_end_date as date)
      ) as drug_exposure_end_date,
      COALESCE(
          GREATEST(
              CASE WHEN idc.service_line_to_date = 'N' THEN NULL ELSE try_cast(idc.service_line_to_date as timestamp) END,
              CASE WHEN idc.service_line_from_date != 'N' AND idc.service_line_from_date IS NOT NULL
                   AND try_cast(idc.days_units_billed as integer) IS NOT NULL
                   THEN try_cast(idc.service_line_from_date as timestamp) + (try_cast(idc.days_units_billed as integer) * INTERVAL '1' DAY)
                   ELSE NULL END
          ),
          try_cast(ihc.reporting_period_end_date as timestamp)
      ) as drug_exposure_end_datetime,
      CASE WHEN idc.service_line_to_date = 'N' THEN NULL
          ELSE try_cast(idc.service_line_to_date as date) END as verbatim_end_date,
      {{ drug_type_mapping['institutional'] }} as drug_type_concept_id,
      cast(null as varchar) as stop_reason,
      0 as refills,
      cast(null as integer) as quantity,
      try_cast(idc.days_units_billed as integer) as days_supply,
      cast(null as varchar) as sig,
      cast(null as integer) as route_concept_id,
      cast(null as integer) as lot_number,
      cast(hash(concat_ws('||',
        ihc.rendering_bill_provider_last,
        coalesce(ihc.rendering_bill_provider_first, ''),
        ihc.rendering_bill_provider_state_1,
        ihc.rendering_bill_provider_4
      ), 'xxhash64') % 1000000000 as varchar) as provider_id,
      cast(idc.bill_id as varchar) as visit_occurrence_id,
      -- visit_detail_id uses same hash as visit_detail table
      cast(hash(concat_ws('||', idc.bill_id, idc.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
      idc.hcpcs_line_procedure_billed as drug_source_value,
      cast(null as integer) as drug_source_concept_id,
      cast(null as varchar) as route_source_value,
      cast(null as varchar) as dose_unit_source_value
    from {{ source('raw', 'institutional_detail_historical') }} idc
    join {{ source('raw', 'institutional_header_historical') }} ihc
      on cast(idc.bill_id as varchar) = cast(ihc.bill_id as varchar)
    join {{ source('omop', 'concept') }} as c
      on c.concept_code = idc.hcpcs_line_procedure_billed
    where c.domain_id = 'Drug'
      and c.vocabulary_id = 'HCPCS'
  )
  {% endset %}
  {% do cte_queries.append(query) %}

  -- Professional detail historical
  {% set query %}
  professional_detail_historical as (
    select
      cast(hash(concat_ws('||', prdc.bill_id, prdc.row_id), 'xxhash64') % 1000000000 as varchar) as drug_exposure_id,
      {{ derive_person_id('prhc') }} as person_id,
      cast(null as integer) as drug_concept_id,
      try_cast(prdc.service_line_from_date as date) as drug_exposure_start_date,
      try_cast(prdc.service_line_from_date as timestamp) as drug_exposure_start_datetime,
      -- Use GREATEST of: service_line_to_date vs (start_date + days_supply), then fallback to visit_end_date
      COALESCE(
          GREATEST(
              try_cast(prdc.service_line_to_date as date),
              CASE WHEN try_cast(prdc.service_line_from_date as date) IS NOT NULL
                   AND try_cast(prdc.days_units_billed as integer) IS NOT NULL
                   THEN try_cast(prdc.service_line_from_date as date) + try_cast(prdc.days_units_billed as integer)
                   ELSE NULL END
          ),
          try_cast(prhc.reporting_period_end_date as date)
      ) as drug_exposure_end_date,
      COALESCE(
          GREATEST(
              try_cast(prdc.service_line_to_date as timestamp),
              CASE WHEN try_cast(prdc.service_line_from_date as timestamp) IS NOT NULL
                   AND try_cast(prdc.days_units_billed as integer) IS NOT NULL
                   THEN try_cast(prdc.service_line_from_date as timestamp) + (try_cast(prdc.days_units_billed as integer) * INTERVAL '1' DAY)
                   ELSE NULL END
          ),
          try_cast(prhc.reporting_period_end_date as timestamp)
      ) as drug_exposure_end_datetime,
      try_cast(prdc.service_line_to_date as date) as verbatim_end_date,
      {{ drug_type_mapping['professional'] }} as drug_type_concept_id,
      cast(null as varchar) as stop_reason,
      0 as refills,
      cast(null as integer) as quantity,
      try_cast(prdc.days_units_billed as integer) as days_supply,
      cast(null as varchar) as sig,
      cast(null as integer) as route_concept_id,
      cast(null as integer) as lot_number,
      cast(hash(concat_ws('||',
        prhc.rendering_bill_provider_last,
        coalesce(prhc.rendering_bill_provider_first, ''),
        prhc.rendering_bill_provider_state_1,
        prhc.rendering_bill_provider_4
      ), 'xxhash64') % 1000000000 as varchar) as provider_id,
      cast(prdc.bill_id as varchar) as visit_occurrence_id,
      -- visit_detail_id uses same hash as visit_detail table
      cast(hash(concat_ws('||', prdc.bill_id, prdc.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
      prdc.hcpcs_line_procedure_billed as drug_source_value,
      cast(null as integer) as drug_source_concept_id,
      cast(null as varchar) as route_source_value,
      cast(null as varchar) as dose_unit_source_value
    from {{ source('raw', 'professional_detail_historical') }} prdc
    join {{ source('raw', 'professional_header_historical') }} prhc
      on cast(prdc.bill_id as varchar) = cast(prhc.bill_id as varchar)
    join {{ source('omop', 'concept') }} as c
      on c.concept_code = prdc.hcpcs_line_procedure_billed
    where c.domain_id = 'Drug'
      and c.vocabulary_id = 'HCPCS'
  )
  {% endset %}
  {% do cte_queries.append(query) %}
{% endif %}

{% set union_queries = [] %}
{% if has_current %}
  {% do union_queries.append('select * from pharmacy_detail_current') %}
  {% do union_queries.append('select * from institutional_detail_current') %}
  {% do union_queries.append('select * from professional_detail_current') %}
{% endif %}
{% if has_historical %}
  {% do union_queries.append('select * from pharmacy_detail_historical') %}
  {% do union_queries.append('select * from institutional_detail_historical') %}
  {% do union_queries.append('select * from professional_detail_historical') %}
{% endif %}

{% if has_current or has_historical %}
with {{ cte_queries | join(",\n") }}

select *
from (
  {{ union_queries | join(" union all ") }}
) as final_result
{% else %}
-- No source tables available - return empty result set with OMOP drug_exposure schema
select
    cast(null as varchar) as drug_exposure_id,
    cast(null as integer) as person_id,
    cast(null as integer) as drug_concept_id,
    cast(null as date) as drug_exposure_start_date,
    cast(null as timestamp) as drug_exposure_start_datetime,
    cast(null as date) as drug_exposure_end_date,
    cast(null as timestamp) as drug_exposure_end_datetime,
    cast(null as date) as verbatim_end_date,
    cast(null as integer) as drug_type_concept_id,
    cast(null as varchar) as stop_reason,
    cast(null as integer) as refills,
    cast(null as float) as quantity,
    cast(null as integer) as days_supply,
    cast(null as varchar) as sig,
    cast(null as integer) as route_concept_id,
    cast(null as integer) as lot_number,
    cast(null as varchar) as provider_id,
    cast(null as varchar) as visit_occurrence_id,
    cast(null as varchar) as visit_detail_id,
    cast(null as varchar) as drug_source_value,
    cast(null as integer) as drug_source_concept_id,
    cast(null as varchar) as route_source_value,
    cast(null as varchar) as dose_unit_source_value
where false
{% endif %}
