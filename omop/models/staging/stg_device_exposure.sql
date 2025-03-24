{% set detail_table_list = [
    ('institutional_detail_current', 'raw', 'institutional'),
    ('institutional_detail_historical', 'raw', 'institutional'),
    ('professional_detail_current', 'raw', 'professional'),
    ('professional_detail_historical', 'raw', 'professional')
] %}

{% set header_mapping = {
    'institutional_detail_current': 'institutional_header_current',
    'institutional_detail_historical': 'institutional_header_historical',
    'professional_detail_current': 'professional_header_current',
    'professional_detail_historical': 'professional_header_historical'
} %}

{% set device_type_mapping = {
    'institutional': 32854,
    'professional': 32872
} %}

{% set cte_queries = [] %}

{% for table, schema, detail_type in detail_table_list %}
  {% if check_table_exists(schema, table) %}
    {% set header_table = header_mapping[table] %}
    {% set device_type_concept_id = device_type_mapping[detail_type] %}
    {% set detail_query %}
    {{ table }} as (
      select 
          cast(
            hash(
              concat_ws('||', detail.bill_id, detail.row_id),
              'xxhash64'
            ) % 1000000000
          as varchar) as device_exposure_id,
          case 
            when header.patient_account_number is null or trim(header.patient_account_number) = ''
              then lpad(
                cast(
                  hash(
                    concat_ws('||',
                      coalesce(header.employee_mailing_city, ''),
                      coalesce(header.employee_mailing_state_code, ''),
                      coalesce(header.employee_mailing_postal_code, ''),
                      coalesce(header.employee_mailing_country, ''),
                      coalesce(cast(header.employee_date_of_birth as varchar), ''),
                      coalesce(header.employee_gender_code, '')
                    ),
                    'xxhash64'
                  ) % 1000000000
                as varchar),
                9,
                '0'
              )
            else header.patient_account_number
          end as person_id,
          cast(null as integer) as device_concept_id,
          cast(detail.service_line_from_date as date) as device_exposure_start_date,
          cast(detail.service_line_from_date as timestamp) as device_exposure_start_datetime,
          cast(detail.service_line_to_date as date) as device_exposure_end_date,
          cast(detail.service_line_to_date as timestamp) as device_exposure_end_datetime,
          {{ device_type_concept_id }} as device_type_concept_id,
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
          cast(null as integer) as visit_detail_id,
          detail.hcpcs_line_procedure_billed as device_source_value,
          cast(null as integer) as device_source_concept_id,
          cast(null as integer) as unit_concept_id,
          cast(null as varchar) as unit_source_value,
          cast(null as varchar) as unit_source_concept_id
      from {{ source(schema, table) }} as detail
      join {{ source(schema, header_table) }} as header
        on cast(detail.bill_id as varchar) = cast(header.bill_id as varchar)
      join {{ source('omop','concept') }} as c
        on c.concept_code = detail.hcpcs_line_procedure_billed
      where c.domain_id = 'Device'
        and c.vocabulary_id = 'HCPCS'
    )
    {% endset %}
    {% do cte_queries.append(detail_query) %}
  {% endif %}
{% endfor %}

{% if cte_queries | length > 0 %}
with {{ cte_queries | join(",\n") }}
{% endif %}

{% set valid_tables = [] %}
{% for table, schema, detail_type in detail_table_list %}
  {% if check_table_exists(schema, table) %}
    {% do valid_tables.append(table) %}
  {% endif %}
{% endfor %}

{% if valid_tables | length > 0 %}
select *
from (
  {% for table in valid_tables %}
    select * from {{ table }}
    {% if not loop.last %}
      union all
    {% endif %}
  {% endfor %}
) as final_result
{% else %}
select null as message
{% endif %}