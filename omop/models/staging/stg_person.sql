-- Check if current or historical data exists (use institutional_header as representative)
{% set has_current = check_table_exists('raw', 'institutional_header_current') %}
{% set has_historical = check_table_exists('raw', 'institutional_header_historical') %}

{% set header_list = [] %}
{% if has_current %}
  {% do header_list.append(('institutional_header_current', 'raw', 'institutional')) %}
  {% do header_list.append(('professional_header_current', 'raw', 'professional')) %}
  {% do header_list.append(('pharmacy_header_current', 'raw', 'pharmacy')) %}
{% endif %}
{% if has_historical %}
  {% do header_list.append(('institutional_header_historical', 'raw', 'institutional')) %}
  {% do header_list.append(('professional_header_historical', 'raw', 'professional')) %}
  {% do header_list.append(('pharmacy_header_historical', 'raw', 'pharmacy')) %}
{% endif %}

{% set cte_queries = [] %}

{% for table, schema, header_type in header_list %}
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
    case
      when employee_gender_code = 'M' then 8507  -- Male
      when employee_gender_code = 'F' then 8532  -- Female
      else 0  -- Unknown
    end as gender_concept_id,
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
    case
      when employee_gender_code = 'M' then 8507  -- Male
      when employee_gender_code = 'F' then 8532  -- Female
      else 0  -- Unknown
    end as gender_concept_id,
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
    case
      when employee_gender_code = 'M' then 8507  -- Male
      when employee_gender_code = 'F' then 8532  -- Female
      else 0  -- Unknown
    end as gender_concept_id,
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
{% endfor %}

{% if has_current or has_historical %}
with {{ cte_queries | join(",\n") }}

select *
from (
  {% for table, schema, header_type in header_list %}
    select * from {{ table }}
    {% if not loop.last %}
      union
    {% endif %}
  {% endfor %}
) as final_result
{% else %}
-- No source tables available - return empty result set with OMOP person schema
select
    cast(null as varchar) as person_id,
    cast(null as integer) as gender_concept_id,
    cast(null as integer) as year_of_birth,
    cast(null as integer) as month_of_birth,
    cast(null as integer) as day_of_birth,
    cast(null as timestamp) as birth_datetime,
    cast(null as integer) as race_concept_id,
    cast(null as integer) as ethnicity_concept_id,
    cast(null as varchar) as location_id,
    cast(null as integer) as provider_id,
    cast(null as varchar) as care_site_id,
    cast(null as varchar) as person_source_value,
    cast(null as varchar) as gender_source_value,
    cast(null as integer) as gender_source_concept_id,
    cast(null as varchar) as race_source_value,
    cast(null as integer) as race_source_concept_id,
    cast(null as varchar) as ethnicity_source_value,
    cast(null as integer) as ethnicity_source_concept_id
where false
{% endif %}
