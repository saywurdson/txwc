{% set header_list = [
  ('institutional_header_current', 'raw', 'institutional'),
  ('institutional_header_historical', 'raw', 'institutional'),
  ('professional_header_current', 'raw', 'professional'),
  ('professional_header_historical', 'raw', 'professional'),
  ('pharmacy_header_current', 'raw', 'pharmacy'),
  ('pharmacy_header_historical', 'raw', 'pharmacy')
] %}

{% set cte_queries = [] %}

{% for table, schema, header_type in header_list %}
  {% if check_table_exists(schema, table) %}
    {% if header_type == 'institutional' %}
      {% set query %}
{{ table }} as (
  select distinct
    case 
      when patient_account_number is null or trim(patient_account_number) = '' then lpad(
        cast(
          (
            hash(
              concat_ws(
                '||',
                coalesce(employee_mailing_city, ''),
                coalesce(employee_mailing_state_code, ''),
                coalesce(employee_mailing_postal_code, ''),
                coalesce(employee_mailing_country, ''),
                coalesce(cast(employee_date_of_birth as varchar), ''),
                coalesce(employee_gender_code, '')
              ),
              'xxhash64'
            ) % 1000000000
          ) as varchar
        ),
        9,
        '0'
      )
      else patient_account_number
    end as person_id,
    cast(null as integer) as gender_concept_id,
    extract(year from TRY_CAST(employee_date_of_birth as date)) as year_of_birth,
    extract(month from TRY_CAST(employee_date_of_birth as date)) as month_of_birth,
    extract(day from TRY_CAST(employee_date_of_birth as date)) as day_of_birth,
    TRY_CAST(employee_date_of_birth as timestamp) as birth_datetime,
    cast(null as integer) as race_concept_id,
    cast(null as integer) as ethnicity_concept_id,
    cast(
      hash(
        concat_ws(
          '||',
          employee_mailing_city,
          employee_mailing_country,
          employee_mailing_postal_code,
          employee_mailing_state_code
        ),
        'xxhash64'
      ) % 1000000000
    as varchar) as location_id,
    cast(null as integer) as provider_id,
    cast(
      hash(
        concat_ws('||', billing_provider_last_name),
        'xxhash64'
      ) % 1000000000
    as varchar) as care_site_id,
    cast(patient_account_number as varchar) as person_source_value,
    cast(employee_gender_code as varchar) as gender_source_value,
    cast(null as integer) as gender_source_concept_id,
    cast(null as varchar) as race_source_value,
    cast(null as integer) as race_source_concept_id,
    cast(null as varchar) as ethnicity_source_value,
    cast(null as integer) as ethnicity_source_concept_id
  from {{ source(schema, table) }}
)
      {% endset %}
    {% elif header_type == 'professional' %}
      {% set query %}
{{ table }} as (
  select distinct
    case 
      when patient_account_number is null or trim(patient_account_number) = '' then lpad(
        cast(
          (
            hash(
              concat_ws(
                '||',
                coalesce(employee_mailing_city, ''),
                coalesce(employee_mailing_state_code, ''),
                coalesce(employee_mailing_postal_code, ''),
                coalesce(employee_mailing_country, ''),
                coalesce(cast(employee_date_of_birth as varchar), ''),
                coalesce(employee_gender_code, '')
              ),
              'xxhash64'
            ) % 1000000000
          ) as varchar
        ),
        9,
        '0'
      )
      else patient_account_number
    end as person_id,
    cast(null as integer) as gender_concept_id,
    extract(year from cast(employee_date_of_birth as date)) as year_of_birth,
    extract(month from cast(employee_date_of_birth as date)) as month_of_birth,
    extract(day from cast(employee_date_of_birth as date)) as day_of_birth,
    cast(employee_date_of_birth as timestamp) as birth_datetime,
    cast(null as integer) as race_concept_id,
    cast(null as integer) as ethnicity_concept_id,
    cast(
      hash(
        concat_ws(
          '||',
          employee_mailing_city,
          employee_mailing_country,
          employee_mailing_postal_code,
          employee_mailing_state_code
        ),
        'xxhash64'
      ) % 1000000000
    as varchar) as location_id,
    cast(null as integer) as provider_id,
    cast(
      hash(
        concat_ws('||', billing_provider_last_name, facility_primary_address),
        'xxhash64'
      ) % 1000000000
    as varchar) as care_site_id,
    cast(patient_account_number as varchar) as person_source_value,
    cast(employee_gender_code as varchar) as gender_source_value,
    cast(null as integer) as gender_source_concept_id,
    cast(null as varchar) as race_source_value,
    cast(null as integer) as race_source_concept_id,
    cast(null as varchar) as ethnicity_source_value,
    cast(null as integer) as ethnicity_source_concept_id
  from {{ source(schema, table) }}
)
      {% endset %}
    {% elif header_type == 'pharmacy' %}
      {% set query %}
{{ table }} as (
  select distinct
    case 
      when patient_account_number is null or trim(patient_account_number) = '' then lpad(
        cast(
          (
            hash(
              concat_ws(
                '||',
                coalesce(employee_mailing_city, ''),
                coalesce(employee_mailing_state_code, ''),
                coalesce(employee_mailing_postal_code, ''),
                coalesce(employee_mailing_country, ''),
                coalesce(cast(employee_date_of_birth as varchar), ''),
                coalesce(employee_gender_code, '')
              ),
              'xxhash64'
            ) % 1000000000
          ) as varchar
        ),
        9,
        '0'
      )
      else patient_account_number
    end as person_id,
    cast(null as integer) as gender_concept_id,
    extract(year from cast(employee_date_of_birth as date)) as year_of_birth,
    extract(month from cast(employee_date_of_birth as date)) as month_of_birth,
    extract(day from cast(employee_date_of_birth as date)) as day_of_birth,
    cast(employee_date_of_birth as timestamp) as birth_datetime,
    cast(null as integer) as race_concept_id,
    cast(null as integer) as ethnicity_concept_id,
    cast(
      hash(
        concat_ws(
          '||',
          employee_mailing_city,
          employee_mailing_country,
          employee_mailing_postal_code,
          employee_mailing_state_code
        ),
        'xxhash64'
      ) % 1000000000
    as varchar) as location_id,
    cast(null as integer) as provider_id,
    cast(
      hash(
        concat_ws('||', billing_provider_last_name, billing_provider_fein),
        'xxhash64'
      ) % 1000000000
    as varchar) as care_site_id,
    cast(patient_account_number as varchar) as person_source_value,
    cast(employee_gender_code as varchar) as gender_source_value,
    cast(null as integer) as gender_source_concept_id,
    cast(null as varchar) as race_source_value,
    cast(null as integer) as race_source_concept_id,
    cast(null as varchar) as ethnicity_source_value,
    cast(null as integer) as ethnicity_source_concept_id
  from {{ source(schema, table) }}
)
      {% endset %}
    {% endif %}
    {% do cte_queries.append(query) %}
  {% endif %}
{% endfor %}

{% set valid_tables = [] %}
{% for table, schema, header_type in header_list %}
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