{% set header_table_list = [
  ('institutional_header_current', 'raw', 'institutional'),
  ('institutional_header_historical', 'raw', 'institutional'),
  ('professional_header_current', 'raw', 'professional'),
  ('professional_header_historical', 'raw', 'professional'),
  ('pharmacy_header_current', 'raw', 'pharmacy'),
  ('pharmacy_header_historical', 'raw', 'pharmacy')
] %}

{% set cte_queries = [] %}

{% for table, schema, header_type in header_table_list %}
  {% if check_table_exists(schema, table) %}
    {% if header_type in ['institutional', 'professional'] %}
      {% set mailing_fields = "employee_mailing_city, employee_mailing_state_code, employee_mailing_postal_code, employee_mailing_country" %}
    {% elif header_type == 'pharmacy' %}
      {% set mailing_fields = "employee_mailing_city, employee_mailing_state_code, employee_mailing_postal_code, employee_mailing_country" %}
    {% endif %}

    {% set mailing_parts = mailing_fields.split(',') | map('trim') | list %}

    {% set query %}
    {{ table }} as (
      select distinct
        cast(
          hash(
            concat_ws('||',
              facility_name,
              facility_primary_address,
              facility_city,
              facility_state_code,
              facility_postal_code,
              facility_country_code
            ),
            'xxhash64'
          ) % 1000000000
        as varchar) as location_id,
        facility_primary_address as address_1,
        {% if header_type == 'professional' %}
        facility_secondary_address as address_2,
        {% else %}
        cast(null as varchar) as address_2,
        {% endif %}
        facility_city as city,
        facility_state_code as state,
        facility_postal_code as zip,
        cast(null as varchar) as county,
        cast(null as varchar) as location_source_value,
        42046186 as country_concept_id,
        facility_country_code as country_source_value,
        cast(null as float) as latitude,
        cast(null as float) as longitude
      from {{ source(schema, table) }}

      union

      select distinct
        cast(
          hash(
            concat_ws('||',
              {{ mailing_parts[0] }},
              {{ mailing_parts[1] }},
              {{ mailing_parts[2] }},
              {{ mailing_parts[3] }}
            ),
            'xxhash64'
          ) % 1000000000
        as varchar) as location_id,
        cast(null as varchar) as address_1,
        cast(null as varchar) as address_2,
        {{ mailing_parts[0] }} as city,
        {{ mailing_parts[1] }} as state,
        {{ mailing_parts[2] }} as zip,
        cast(null as varchar) as county,
        cast(null as varchar) as location_source_value,
        42046186 as country_concept_id,
        {{ mailing_parts[3] }} as country_source_value,
        cast(null as float) as latitude,
        cast(null as float) as longitude
      from {{ source(schema, table) }}
    )
    {% endset %}
    {% do cte_queries.append(query) %}
  {% endif %}
{% endfor %}

{% set valid_tables = [] %}
{% for table, schema, header_type in header_table_list %}
  {% if check_table_exists(schema, table) %}
    {% do valid_tables.append(table) %}
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
      union
    {% endif %}
  {% endfor %}
) as final_result