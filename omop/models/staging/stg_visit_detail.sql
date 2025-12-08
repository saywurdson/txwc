{% set detail_table_list = [
    ('institutional_detail_current', 'institutional_header_current', 'institutional'),
    ('institutional_detail_historical', 'institutional_header_historical', 'institutional'),
    ('professional_detail_current', 'professional_header_current', 'professional'),
    ('professional_detail_historical', 'professional_header_historical', 'professional'),
    ('pharmacy_detail_current', 'pharmacy_header_current', 'pharmacy'),
    ('pharmacy_detail_historical', 'pharmacy_header_historical', 'pharmacy')
] %}

{% set visit_detail_type_mapping = {
    'institutional': 32855,
    'professional': 32873,
    'pharmacy': 32869
} %}

{% set visit_detail_concept_mapping = {
    'institutional': 8717,
    'professional': 8716,
    'pharmacy': 38004338
} %}

{% set cte_queries = [] %}

{% for detail_table, header_table, detail_type in detail_table_list %}
  {% if check_table_exists('raw', detail_table) and check_table_exists('raw', header_table) %}
    {% set visit_detail_type_concept_id = visit_detail_type_mapping[detail_type] %}
    {% set visit_detail_concept_id = visit_detail_concept_mapping[detail_type] %}

    {% set query %}
    {{ detail_table }} as (
      select
        cast(hash(concat_ws('||', d.bill_id, d.row_id), 'xxhash64') % 1000000000 as varchar) as visit_detail_id,
        case
          when h.patient_account_number is null or trim(h.patient_account_number) = '' then lpad(
            cast(
              hash(concat_ws('||',
                coalesce(h.employee_mailing_city, ''),
                coalesce(h.employee_mailing_state_code, ''),
                coalesce(h.employee_mailing_postal_code, ''),
                coalesce(h.employee_mailing_country, ''),
                coalesce(cast(h.employee_date_of_birth as varchar), ''),
                coalesce(h.employee_gender_code, '')
              ), 'xxhash64') % 1000000000 as varchar
            ),
            9,
            '0'
          )
          else h.patient_account_number
        end as person_id,
        {{ visit_detail_concept_id }} as visit_detail_concept_id,
        {% if detail_type == 'institutional' %}
        CASE WHEN d.service_line_from_date = 'N' THEN NULL
            ELSE try_cast(d.service_line_from_date as date) END as visit_detail_start_date,
        CASE WHEN d.service_line_from_date = 'N' THEN NULL
            ELSE try_cast(d.service_line_from_date as timestamp) END as visit_detail_start_datetime,
        CASE WHEN d.service_line_to_date = 'N' THEN NULL
            ELSE try_cast(d.service_line_to_date as date) END as visit_detail_end_date,
        CASE WHEN d.service_line_to_date = 'N' THEN NULL
            ELSE try_cast(d.service_line_to_date as timestamp) END as visit_detail_end_datetime,
        {% elif detail_type == 'pharmacy' %}
        coalesce(try_cast(d.service_line_from_date as date), try_cast(d.prescription_line_date as date)) as visit_detail_start_date,
        coalesce(try_cast(d.service_line_from_date as timestamp), try_cast(d.prescription_line_date as timestamp)) as visit_detail_start_datetime,
        try_cast(d.service_line_to_date as date) as visit_detail_end_date,
        try_cast(d.service_line_to_date as timestamp) as visit_detail_end_datetime,
        {% else %}
        try_cast(d.service_line_from_date as date) as visit_detail_start_date,
        try_cast(d.service_line_from_date as timestamp) as visit_detail_start_datetime,
        try_cast(d.service_line_to_date as date) as visit_detail_end_date,
        try_cast(d.service_line_to_date as timestamp) as visit_detail_end_datetime,
        {% endif %}
        {{ visit_detail_type_concept_id }} as visit_detail_type_concept_id,
        cast(hash(concat_ws('||',
          h.rendering_bill_provider_last,
          coalesce(h.rendering_bill_provider_first, ''),
          h.rendering_bill_provider_state_1,
          h.rendering_bill_provider_4
        ), 'xxhash64') % 1000000000 as varchar) as provider_id,
        cast(
          hash(concat_ws('||',
            {% if detail_type == 'pharmacy' %}
              h.billing_provider_last_name,
              h.billing_provider_fein
            {% elif detail_type == 'professional' %}
              h.billing_provider_last_name,
              h.facility_primary_address
            {% else %}
              h.billing_provider_last_name
            {% endif %}
          ), 'xxhash64') % 1000000000
        as varchar) as care_site_id,
        cast(d.bill_id as varchar) as visit_occurrence_id,
        cast(null as varchar) as visit_detail_source_value,
        cast(null as integer) as visit_detail_source_concept_id,
        cast(null as integer) as admitted_from_concept_id,
        cast(null as varchar) as admitted_from_source_value,
        cast(null as integer) as discharged_to_concept_id,
        cast(null as varchar) as discharged_to_source_value,
        cast(null as varchar) as preceding_visit_detail_id,
        cast(null as varchar) as parent_visit_detail_id
      from {{ source('raw', detail_table) }} d
      join {{ source('raw', header_table) }} h
        on cast(d.bill_id as varchar) = cast(h.bill_id as varchar)
    )
    {% endset %}
    {% do cte_queries.append(query) %}
  {% endif %}
{% endfor %}

{% set valid_tables = [] %}
{% for detail_table, header_table, detail_type in detail_table_list %}
  {% if check_table_exists('raw', detail_table) and check_table_exists('raw', header_table) %}
    {% do valid_tables.append(detail_table) %}
  {% endif %}
{% endfor %}

{% if cte_queries | length > 0 %}
with {{ cte_queries | join(",\n") }}
{% endif %}

select *
from (
  {% for table in valid_tables %}
    select * from {{ table }}
    {% if not loop.last %}
      union all
    {% endif %}
  {% endfor %}
) as final_result
