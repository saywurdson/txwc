{% set header_list = [
  ('institutional_header_current', 'raw', 'institutional'),
  ('institutional_header_historical', 'raw', 'institutional'),
  ('professional_header_current', 'raw', 'professional'),
  ('professional_header_historical', 'raw', 'professional'),
  ('pharmacy_header_current', 'raw', 'pharmacy'),
  ('pharmacy_header_historical', 'raw', 'pharmacy')
] %}

{% set cte_queries = [] %}
{% for table, schema, htype in header_list %}
  {% if check_table_exists(schema, table) %}
    {% if htype == 'institutional' %}
      {% set query %}
{{ table }} as (
  select distinct
    cast(
      hash(
        concat_ws(
          '||',
          rendering_bill_provider_last,
          coalesce(rendering_bill_provider_first, ''),
          rendering_bill_provider_state_1,
          rendering_bill_provider_4
        ),
        'xxhash64'
      ) % 1000000000
    as varchar) as provider_id,
    concat(
      rendering_bill_provider_last, 
      case 
        when rendering_bill_provider_first is not null 
          then concat(', ', rendering_bill_provider_first)
        else ''
      end
    ) as provider_name,
    rendering_bill_provider_4 as npi,
    cast(null as varchar) as dea,
    cast(null as integer) as specialty_concept_id,
    cast(
      hash(
        concat_ws(
          '||',
          billing_provider_last_name
        ),
        'xxhash64'
      ) % 1000000000
    as varchar) as care_site_id,
    cast(null as integer) as year_of_birth,
    cast(null as integer) as gender_concept_id,
    rendering_bill_provider_state_1 as provider_source_value,
    cast(null as varchar) as specialty_source_value,
    cast(null as integer) as specialty_source_concept_id,
    cast(null as varchar) as gender_source_value,
    cast(null as integer) as gender_source_concept_id
  from {{ source(schema, table) }}
)
      {% endset %}
    {% elif htype == 'professional' %}
      {% set query %}
{{ table }} as (
  select distinct
    cast(
      hash(
        concat_ws(
          '||',
          rendering_bill_provider_last,
          coalesce(rendering_bill_provider_first, ''),
          rendering_bill_provider_state_1,
          rendering_bill_provider_4
        ),
        'xxhash64'
      ) % 1000000000
    as varchar) as provider_id,
    concat(
      rendering_bill_provider_last, 
      case 
        when rendering_bill_provider_first is not null 
          then concat(', ', rendering_bill_provider_first)
        else ''
      end
    ) as provider_name,
    rendering_bill_provider_4 as npi,
    cast(null as varchar) as dea,
    cast(null as integer) as specialty_concept_id,
    cast(
      hash(
        concat_ws(
          '||',
          billing_provider_last_name,
          facility_primary_address
        ),
        'xxhash64'
      ) % 1000000000
    as varchar) as care_site_id,
    cast(null as integer) as year_of_birth,
    cast(null as integer) as gender_concept_id,
    rendering_bill_provider_state_1 as provider_source_value,
    cast(null as varchar) as specialty_source_value,
    cast(null as integer) as specialty_source_concept_id,
    cast(null as varchar) as gender_source_value,
    cast(null as integer) as gender_source_concept_id
  from {{ source(schema, table) }}
)
      {% endset %}
    {% elif htype == 'pharmacy' %}
      {% set query %}
{{ table }} as (
  select distinct
    cast(
      hash(
        concat_ws(
          '||',
          rendering_bill_provider_last,
          coalesce(rendering_bill_provider_first, ''),
          rendering_bill_provider_state_1,
          rendering_bill_provider_4
        ),
        'xxhash64'
      ) % 1000000000
    as varchar) as provider_id,
    concat(
      rendering_bill_provider_last, 
      case 
        when rendering_bill_provider_first is not null 
          then concat(', ', rendering_bill_provider_first)
        else ''
      end
    ) as provider_name,
    rendering_bill_provider_4 as npi,
    cast(null as varchar) as dea,
    cast(null as integer) as specialty_concept_id,
    cast(
      hash(
        concat_ws(
          '||',
          billing_provider_last_name,
          billing_provider_fein
        ),
        'xxhash64'
      ) % 1000000000
    as varchar) as care_site_id,
    cast(null as integer) as year_of_birth,
    cast(null as integer) as gender_concept_id,
    rendering_bill_provider_state_1 as provider_source_value,
    cast(null as varchar) as specialty_source_value,
    cast(null as integer) as specialty_source_concept_id,
    cast(null as varchar) as gender_source_value,
    cast(null as integer) as gender_source_concept_id
  from {{ source(schema, table) }}
)
      {% endset %}
    {% endif %}
    {% do cte_queries.append(query) %}
  {% endif %}
{% endfor %}

{% set valid_tables = [] %}
{% for table, schema, htype in header_list %}
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