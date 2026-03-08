-- Check if current or historical data exists (use institutional_header as representative)
{% set has_current = check_table_exists('raw', 'institutional_header_current') %}
{% set has_historical = check_table_exists('raw', 'institutional_header_historical') %}

{% set device_type_mapping = {
    'institutional': 32854,
    'professional': 32872
} %}

{% set cte_queries = [] %}

{% if has_current %}
  -- Institutional detail current
  {% set query %}
  institutional_detail_current as (
    select
        cast(
          hash(
            concat_ws('||', detail.bill_id, detail.row_id),
            'xxhash64'
          ) % 1000000000
        as varchar) as device_exposure_id,
        {{ derive_person_id('header') }} as person_id,
        cast(null as integer) as device_concept_id,
        CASE WHEN detail.service_line_from_date = 'N' THEN NULL
            ELSE try_cast(detail.service_line_from_date as date) END as device_exposure_start_date,
        CASE WHEN detail.service_line_from_date = 'N' THEN NULL
            ELSE try_cast(detail.service_line_from_date as timestamp) END as device_exposure_start_datetime,
        -- Fallback to visit end date (reporting_period_end_date) when service_line_to_date is null
        COALESCE(
            CASE WHEN detail.service_line_to_date = 'N' THEN NULL ELSE try_cast(detail.service_line_to_date as date) END,
            cast(header.reporting_period_end_date as date)
        ) as device_exposure_end_date,
        COALESCE(
            CASE WHEN detail.service_line_to_date = 'N' THEN NULL ELSE try_cast(detail.service_line_to_date as timestamp) END,
            cast(header.reporting_period_end_date as timestamp)
        ) as device_exposure_end_datetime,
        {{ device_type_mapping['institutional'] }} as device_type_concept_id,
        cast(null as varchar) as unique_device_id,
        cast(null as varchar) as production_id,
        1 as quantity,
        cast(
          hash(
            concat_ws('||',
              header.rendering_bill_provider_last,
              coalesce(header.rendering_bill_provider_first, ''),
              header.rendering_bill_provider_state_1,
              header.rendering_bill_provider_4
            ),
            'xxhash64'
          ) % 1000000000
        as varchar) as provider_id,
        cast(detail.bill_id as varchar) as visit_occurrence_id,
        -- Detail-based devices link to visit_detail via bill_id + row_id hash
        cast(hash(concat_ws('||', detail.bill_id, detail.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
        detail.hcpcs_line_procedure_billed as device_source_value,
        cast(null as integer) as device_source_concept_id,
        cast(null as integer) as unit_concept_id,
        cast(null as varchar) as unit_source_value,
        cast(null as varchar) as unit_source_concept_id
    from {{ source('raw', 'institutional_detail_current') }} as detail
    join {{ source('raw', 'institutional_header_current') }} as header
      on cast(detail.bill_id as varchar) = cast(header.bill_id as varchar)
    join {{ source('omop','concept') }} as c
      on c.concept_code = detail.hcpcs_line_procedure_billed
    where c.domain_id = 'Device'
      and c.vocabulary_id = 'HCPCS'
  )
  {% endset %}
  {% do cte_queries.append(query) %}

  -- Professional detail current
  {% set query %}
  professional_detail_current as (
    select
        cast(
          hash(
            concat_ws('||', detail.bill_id, detail.row_id),
            'xxhash64'
          ) % 1000000000
        as varchar) as device_exposure_id,
        {{ derive_person_id('header') }} as person_id,
        cast(null as integer) as device_concept_id,
        cast(detail.service_line_from_date as date) as device_exposure_start_date,
        cast(detail.service_line_from_date as timestamp) as device_exposure_start_datetime,
        -- Fallback to visit end date (reporting_period_end_date) when service_line_to_date is null
        COALESCE(
            cast(detail.service_line_to_date as date),
            cast(header.reporting_period_end_date as date)
        ) as device_exposure_end_date,
        COALESCE(
            cast(detail.service_line_to_date as timestamp),
            cast(header.reporting_period_end_date as timestamp)
        ) as device_exposure_end_datetime,
        {{ device_type_mapping['professional'] }} as device_type_concept_id,
        cast(null as varchar) as unique_device_id,
        cast(null as varchar) as production_id,
        1 as quantity,
        cast(
          hash(
            concat_ws('||',
              header.rendering_bill_provider_last,
              coalesce(header.rendering_bill_provider_first, ''),
              header.rendering_bill_provider_state_1,
              header.rendering_bill_provider_4
            ),
            'xxhash64'
          ) % 1000000000
        as varchar) as provider_id,
        cast(detail.bill_id as varchar) as visit_occurrence_id,
        -- Detail-based devices link to visit_detail via bill_id + row_id hash
        cast(hash(concat_ws('||', detail.bill_id, detail.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
        detail.hcpcs_line_procedure_billed as device_source_value,
        cast(null as integer) as device_source_concept_id,
        cast(null as integer) as unit_concept_id,
        cast(null as varchar) as unit_source_value,
        cast(null as varchar) as unit_source_concept_id
    from {{ source('raw', 'professional_detail_current') }} as detail
    join {{ source('raw', 'professional_header_current') }} as header
      on cast(detail.bill_id as varchar) = cast(header.bill_id as varchar)
    join {{ source('omop','concept') }} as c
      on c.concept_code = detail.hcpcs_line_procedure_billed
    where c.domain_id = 'Device'
      and c.vocabulary_id = 'HCPCS'
  )
  {% endset %}
  {% do cte_queries.append(query) %}
{% endif %}

{% if has_historical %}
  -- Institutional detail historical
  {% set query %}
  institutional_detail_historical as (
    select
        cast(
          hash(
            concat_ws('||', detail.bill_id, detail.row_id),
            'xxhash64'
          ) % 1000000000
        as varchar) as device_exposure_id,
        {{ derive_person_id('header') }} as person_id,
        cast(null as integer) as device_concept_id,
        CASE WHEN detail.service_line_from_date = 'N' THEN NULL
            ELSE try_cast(detail.service_line_from_date as date) END as device_exposure_start_date,
        CASE WHEN detail.service_line_from_date = 'N' THEN NULL
            ELSE try_cast(detail.service_line_from_date as timestamp) END as device_exposure_start_datetime,
        -- Fallback to visit end date (reporting_period_end_date) when service_line_to_date is null
        COALESCE(
            CASE WHEN detail.service_line_to_date = 'N' THEN NULL ELSE try_cast(detail.service_line_to_date as date) END,
            cast(header.reporting_period_end_date as date)
        ) as device_exposure_end_date,
        COALESCE(
            CASE WHEN detail.service_line_to_date = 'N' THEN NULL ELSE try_cast(detail.service_line_to_date as timestamp) END,
            cast(header.reporting_period_end_date as timestamp)
        ) as device_exposure_end_datetime,
        {{ device_type_mapping['institutional'] }} as device_type_concept_id,
        cast(null as varchar) as unique_device_id,
        cast(null as varchar) as production_id,
        1 as quantity,
        cast(
          hash(
            concat_ws('||',
              header.rendering_bill_provider_last,
              coalesce(header.rendering_bill_provider_first, ''),
              header.rendering_bill_provider_state_1,
              header.rendering_bill_provider_4
            ),
            'xxhash64'
          ) % 1000000000
        as varchar) as provider_id,
        cast(detail.bill_id as varchar) as visit_occurrence_id,
        -- Detail-based devices link to visit_detail via bill_id + row_id hash
        cast(hash(concat_ws('||', detail.bill_id, detail.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
        detail.hcpcs_line_procedure_billed as device_source_value,
        cast(null as integer) as device_source_concept_id,
        cast(null as integer) as unit_concept_id,
        cast(null as varchar) as unit_source_value,
        cast(null as varchar) as unit_source_concept_id
    from {{ source('raw', 'institutional_detail_historical') }} as detail
    join {{ source('raw', 'institutional_header_historical') }} as header
      on cast(detail.bill_id as varchar) = cast(header.bill_id as varchar)
    join {{ source('omop','concept') }} as c
      on c.concept_code = detail.hcpcs_line_procedure_billed
    where c.domain_id = 'Device'
      and c.vocabulary_id = 'HCPCS'
  )
  {% endset %}
  {% do cte_queries.append(query) %}

  -- Professional detail historical
  {% set query %}
  professional_detail_historical as (
    select
        cast(
          hash(
            concat_ws('||', detail.bill_id, detail.row_id),
            'xxhash64'
          ) % 1000000000
        as varchar) as device_exposure_id,
        {{ derive_person_id('header') }} as person_id,
        cast(null as integer) as device_concept_id,
        cast(detail.service_line_from_date as date) as device_exposure_start_date,
        cast(detail.service_line_from_date as timestamp) as device_exposure_start_datetime,
        -- Fallback to visit end date (reporting_period_end_date) when service_line_to_date is null
        COALESCE(
            cast(detail.service_line_to_date as date),
            cast(header.reporting_period_end_date as date)
        ) as device_exposure_end_date,
        COALESCE(
            cast(detail.service_line_to_date as timestamp),
            cast(header.reporting_period_end_date as timestamp)
        ) as device_exposure_end_datetime,
        {{ device_type_mapping['professional'] }} as device_type_concept_id,
        cast(null as varchar) as unique_device_id,
        cast(null as varchar) as production_id,
        1 as quantity,
        cast(
          hash(
            concat_ws('||',
              header.rendering_bill_provider_last,
              coalesce(header.rendering_bill_provider_first, ''),
              header.rendering_bill_provider_state_1,
              header.rendering_bill_provider_4
            ),
            'xxhash64'
          ) % 1000000000
        as varchar) as provider_id,
        cast(detail.bill_id as varchar) as visit_occurrence_id,
        -- Detail-based devices link to visit_detail via bill_id + row_id hash
        cast(hash(concat_ws('||', detail.bill_id, detail.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
        detail.hcpcs_line_procedure_billed as device_source_value,
        cast(null as integer) as device_source_concept_id,
        cast(null as integer) as unit_concept_id,
        cast(null as varchar) as unit_source_value,
        cast(null as varchar) as unit_source_concept_id
    from {{ source('raw', 'professional_detail_historical') }} as detail
    join {{ source('raw', 'professional_header_historical') }} as header
      on cast(detail.bill_id as varchar) = cast(header.bill_id as varchar)
    join {{ source('omop','concept') }} as c
      on c.concept_code = detail.hcpcs_line_procedure_billed
    where c.domain_id = 'Device'
      and c.vocabulary_id = 'HCPCS'
  )
  {% endset %}
  {% do cte_queries.append(query) %}
{% endif %}

{% set union_queries = [] %}
{% if has_current %}
  {% do union_queries.append('select * from institutional_detail_current') %}
  {% do union_queries.append('select * from professional_detail_current') %}
{% endif %}
{% if has_historical %}
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
-- No source tables available - return empty result set with OMOP device_exposure schema
select
    cast(null as varchar) as device_exposure_id,
    cast(null as integer) as person_id,
    cast(null as integer) as device_concept_id,
    cast(null as date) as device_exposure_start_date,
    cast(null as timestamp) as device_exposure_start_datetime,
    cast(null as date) as device_exposure_end_date,
    cast(null as timestamp) as device_exposure_end_datetime,
    cast(null as integer) as device_type_concept_id,
    cast(null as varchar) as unique_device_id,
    cast(null as varchar) as production_id,
    cast(null as integer) as quantity,
    cast(null as varchar) as provider_id,
    cast(null as varchar) as visit_occurrence_id,
    cast(null as varchar) as visit_detail_id,
    cast(null as varchar) as device_source_value,
    cast(null as integer) as device_source_concept_id,
    cast(null as integer) as unit_concept_id,
    cast(null as varchar) as unit_source_value,
    cast(null as varchar) as unit_source_concept_id
where false
{% endif %}
