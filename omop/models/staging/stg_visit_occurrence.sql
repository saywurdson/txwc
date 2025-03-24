{% set exists_i_current = check_table_exists('raw', 'institutional_header_current') %}
{% set exists_i_historical = check_table_exists('raw', 'institutional_header_historical') %}
{% set exists_pr_current = check_table_exists('raw', 'professional_header_current') %}
{% set exists_pr_historical = check_table_exists('raw', 'professional_header_historical') %}
{% set exists_ph_current = check_table_exists('raw', 'pharmacy_header_current') %}
{% set exists_ph_historical = check_table_exists('raw', 'pharmacy_header_historical') %}

{% set cte_definitions = [] %}

{% if exists_i_current %}
  {% set query %}
institutional_header_current as (
  select distinct
      cast(bill_id as varchar) as visit_occurrence_id,
      case 
          when patient_account_number is null or trim(patient_account_number) = '' then
              lpad(
                cast(
                  (hash(concat_ws('||',
                      coalesce(employee_mailing_city, ''),
                      coalesce(employee_mailing_state_code, ''),
                      coalesce(employee_mailing_postal_code, ''),
                      coalesce(employee_mailing_country, ''),
                      coalesce(cast(employee_date_of_birth as varchar), ''),
                      coalesce(employee_gender_code, '')
                  ), 'xxhash64') % 1000000000) as varchar
                ),
                9,
                '0'
              )
          else patient_account_number
      end as person_id,
      cast(null as integer) as visit_concept_id,
      cast(service_bill_from_date as date) as visit_start_date,
      cast(service_bill_from_date as timestamp) as visit_start_datetime,
      cast(service_bill_to_date as date) as visit_end_date,
      cast(service_bill_to_date as timestamp) as visit_end_datetime,
      32855 as visit_type_concept_id,
      cast(
        hash(concat_ws('||', rendering_bill_provider_last,
          coalesce(rendering_bill_provider_first, ''),
          rendering_bill_provider_state_1,
          rendering_bill_provider_4), 'xxhash64') % 1000000000 as varchar
      ) as provider_id,
      cast(
        hash(concat_ws('||', billing_provider_last_name), 'xxhash64') % 1000000000 as varchar
      ) as care_site_id,
      cast(facility_code as varchar) as visit_source_value,
      cast(null as integer) as visit_source_concept_id,
      cast(null as integer) as admitted_from_source_value,
      cast(null as integer) as discharged_to_concept_id,
      cast(null as integer) as discharged_to_source_value,
      cast(null as integer) as preceding_visit_occurrence_id
  from {{ source('raw', 'institutional_header_current') }}
)
  {% endset %}
  {% do cte_definitions.append(query) %}
{% endif %}

{% if exists_i_historical %}
  {% set query %}
institutional_header_historical as (
  select distinct
      cast(bill_id as varchar) as visit_occurrence_id,
      case 
          when patient_account_number is null or trim(patient_account_number) = '' then
              lpad(
                cast(
                  (hash(concat_ws('||',
                      coalesce(employee_mailing_city, ''),
                      coalesce(employee_mailing_state_code, ''),
                      coalesce(employee_mailing_postal_code, ''),
                      coalesce(employee_mailing_country, ''),
                      coalesce(cast(employee_date_of_birth as varchar), ''),
                      coalesce(employee_gender_code, '')
                  ), 'xxhash64') % 1000000000) as varchar
                ),
                9,
                '0'
              )
          else patient_account_number
      end as person_id,
      cast(null as integer) as visit_concept_id,
      cast(service_bill_from_date as date) as visit_start_date,
      cast(service_bill_from_date as timestamp) as visit_start_datetime,
      cast(service_bill_to_date as date) as visit_end_date,
      cast(service_bill_to_date as timestamp) as visit_end_datetime,
      32855 as visit_type_concept_id,
      cast(
        hash(concat_ws('||', rendering_bill_provider_last,
          coalesce(rendering_bill_provider_first, ''),
          rendering_bill_provider_state_1,
          rendering_bill_provider_4), 'xxhash64') % 1000000000 as varchar
      ) as provider_id,
      cast(
        hash(concat_ws('||', billing_provider_last_name), 'xxhash64') % 1000000000 as varchar
      ) as care_site_id,
      cast(facility_code as varchar) as visit_source_value,
      cast(null as integer) as visit_source_concept_id,
      cast(null as integer) as admitted_from_source_value,
      cast(null as integer) as discharged_to_concept_id,
      cast(null as integer) as discharged_to_source_value,
      cast(null as integer) as preceding_visit_occurrence_id
  from {{ source('raw', 'institutional_header_historical') }}
)
  {% endset %}
  {% do cte_definitions.append(query) %}
{% endif %}

{% if exists_pr_current %}
  {% set query %}
professional_header_current as (
  select distinct
      cast(bill_id as varchar) as visit_occurrence_id,
      case 
          when patient_account_number is null or trim(patient_account_number) = '' then
              lpad(
                cast(
                  (hash(concat_ws('||',
                      coalesce(employee_mailing_city, ''),
                      coalesce(employee_mailing_state_code, ''),
                      coalesce(employee_mailing_postal_code, ''),
                      coalesce(employee_mailing_country, ''),
                      coalesce(cast(employee_date_of_birth as varchar), ''),
                      coalesce(employee_gender_code, '')
                  ), 'xxhash64') % 1000000000) as varchar
                ),
                9,
                '0'
              )
          else patient_account_number
      end as person_id,
      cast(null as integer) as visit_concept_id,
      cast(service_bill_from_date as date) as visit_start_date,
      cast(service_bill_from_date as timestamp) as visit_start_datetime,
      cast(service_bill_to_date as date) as visit_end_date,
      cast(service_bill_to_date as timestamp) as visit_end_datetime,
      32873 as visit_type_concept_id,
      cast(
        hash(concat_ws('||', rendering_bill_provider_last,
          coalesce(rendering_bill_provider_first, ''),
          rendering_bill_provider_state_1,
          rendering_bill_provider_4), 'xxhash64') % 1000000000 as varchar
      ) as provider_id,
      cast(
        hash(concat_ws('||', billing_provider_last_name, facility_primary_address), 'xxhash64') % 1000000000 as varchar
      ) as care_site_id,
      cast(place_of_service_bill_code as varchar) as visit_source_value,
      cast(null as integer) as visit_source_concept_id,
      cast(null as integer) as admitted_from_source_value,
      cast(null as integer) as discharged_to_concept_id,
      cast(null as integer) as discharged_to_source_value,
      cast(null as integer) as preceding_visit_occurrence_id
  from {{ source('raw', 'professional_header_current') }}
)
  {% endset %}
  {% do cte_definitions.append(query) %}
{% endif %}

{% if exists_pr_historical %}
  {% set query %}
professional_header_historical as (
  select distinct
      cast(bill_id as varchar) as visit_occurrence_id,
      case 
          when patient_account_number is null or trim(patient_account_number) = '' then
              lpad(
                cast(
                  (hash(concat_ws('||',
                      coalesce(employee_mailing_city, ''),
                      coalesce(employee_mailing_state_code, ''),
                      coalesce(employee_mailing_postal_code, ''),
                      coalesce(employee_mailing_country, ''),
                      coalesce(cast(employee_date_of_birth as varchar), ''),
                      coalesce(employee_gender_code, '')
                  ), 'xxhash64') % 1000000000) as varchar
                ),
                9,
                '0'
              )
          else patient_account_number
      end as person_id,
      cast(null as integer) as visit_concept_id,
      cast(service_bill_from_date as date) as visit_start_date,
      cast(service_bill_from_date as timestamp) as visit_start_datetime,
      cast(service_bill_to_date as date) as visit_end_date,
      cast(service_bill_to_date as timestamp) as visit_end_datetime,
      32873 as visit_type_concept_id,
      cast(
        hash(concat_ws('||', rendering_bill_provider_last,
          coalesce(rendering_bill_provider_first, ''),
          rendering_bill_provider_state_1,
          rendering_bill_provider_4), 'xxhash64') % 1000000000 as varchar
      ) as provider_id,
      cast(
        hash(concat_ws('||', billing_provider_last_name, facility_primary_address), 'xxhash64') % 1000000000 as varchar
      ) as care_site_id,
      cast(place_of_service_bill_code as varchar) as visit_source_value,
      cast(null as integer) as visit_source_concept_id,
      cast(null as integer) as admitted_from_source_value,
      cast(null as integer) as discharged_to_concept_id,
      cast(null as integer) as discharged_to_source_value,
      cast(null as integer) as preceding_visit_occurrence_id
  from {{ source('raw', 'professional_header_historical') }}
)
  {% endset %}
  {% do cte_definitions.append(query) %}
{% endif %}

{% if exists_ph_current %}
  {% set query %}
pharmacy_header_current as (
  select distinct
      cast(bill_id as varchar) as visit_occurrence_id,
      case 
          when patient_account_number is null or trim(patient_account_number) = '' then
              lpad(
                cast(
                  (hash(concat_ws('||',
                      coalesce(employee_mailing_city, ''),
                      coalesce(employee_mailing_state_code, ''),
                      coalesce(employee_mailing_postal_code, ''),
                      coalesce(employee_mailing_country, ''),
                      coalesce(cast(employee_date_of_birth as varchar), ''),
                      coalesce(employee_gender_code, '')
                  ), 'xxhash64') % 1000000000) as varchar
                ),
                9,
                '0'
              )
          else patient_account_number
      end as person_id,
      cast(null as integer) as visit_concept_id,
      cast(service_bill_from_date as date) as visit_start_date,
      cast(service_bill_from_date as timestamp) as visit_start_datetime,
      cast(service_bill_to_date as date) as visit_end_date,
      cast(service_bill_to_date as timestamp) as visit_end_datetime,
      32869 as visit_type_concept_id,
      cast(
        hash(concat_ws('||', rendering_bill_provider_last,
          coalesce(rendering_bill_provider_first, ''),
          rendering_bill_provider_state_1,
          rendering_bill_provider_4), 'xxhash64') % 1000000000 as varchar
      ) as provider_id,
      cast(
        hash(concat_ws('||', billing_provider_last_name, billing_provider_fein), 'xxhash64') % 1000000000 as varchar
      ) as care_site_id,
      cast(place_of_service_bill_code as varchar) as visit_source_value,
      cast(null as integer) as visit_source_concept_id,
      cast(null as integer) as admitted_from_source_value,
      cast(null as integer) as discharged_to_concept_id,
      cast(null as integer) as discharged_to_source_value,
      cast(null as integer) as preceding_visit_occurrence_id
  from {{ source('raw', 'pharmacy_header_current') }}
)
  {% endset %}
  {% do cte_definitions.append(query) %}
{% endif %}

{% if exists_ph_historical %}
  {% set query %}
pharmacy_header_historical as (
  select distinct
      cast(bill_id as varchar) as visit_occurrence_id,
      case 
          when patient_account_number is null or trim(patient_account_number) = '' then
              lpad(
                cast(
                  (hash(concat_ws('||',
                      coalesce(employee_mailing_city, ''),
                      coalesce(employee_mailing_state_code, ''),
                      coalesce(employee_mailing_postal_code, ''),
                      coalesce(employee_mailing_country, ''),
                      coalesce(cast(employee_date_of_birth as varchar), ''),
                      coalesce(employee_gender_code, '')
                  ), 'xxhash64') % 1000000000) as varchar
                ),
                9,
                '0'
              )
          else patient_account_number
      end as person_id,
      cast(null as integer) as visit_concept_id,
      cast(service_bill_from_date as date) as visit_start_date,
      cast(service_bill_from_date as timestamp) as visit_start_datetime,
      cast(service_bill_to_date as date) as visit_end_date,
      cast(service_bill_to_date as timestamp) as visit_end_datetime,
      32869 as visit_type_concept_id,
      cast(
        hash(concat_ws('||', rendering_bill_provider_last,
          coalesce(rendering_bill_provider_first, ''),
          rendering_bill_provider_state_1,
          rendering_bill_provider_4), 'xxhash64') % 1000000000 as varchar
      ) as provider_id,
      cast(
        hash(concat_ws('||', billing_provider_last_name, billing_provider_fein), 'xxhash64') % 1000000000 as varchar
      ) as care_site_id,
      cast(place_of_service_bill_code as varchar) as visit_source_value,
      cast(null as integer) as visit_source_concept_id,
      cast(null as integer) as admitted_from_source_value,
      cast(null as integer) as discharged_to_concept_id,
      cast(null as integer) as discharged_to_source_value,
      cast(null as integer) as preceding_visit_occurrence_id
  from {{ source('raw', 'pharmacy_header_historical') }}
)
  {% endset %}
  {% do cte_definitions.append(query) %}
{% endif %}

{% set union_queries = [] %}
{% if exists_i_current %}
  {% do union_queries.append('select * from institutional_header_current') %}
{% endif %}
{% if exists_i_historical %}
  {% do union_queries.append('select * from institutional_header_historical') %}
{% endif %}
{% if exists_pr_current %}
  {% do union_queries.append('select * from professional_header_current') %}
{% endif %}
{% if exists_pr_historical %}
  {% do union_queries.append('select * from professional_header_historical') %}
{% endif %}
{% if exists_ph_current %}
  {% do union_queries.append('select * from pharmacy_header_current') %}
{% endif %}
{% if exists_ph_historical %}
  {% do union_queries.append('select * from pharmacy_header_historical') %}
{% endif %}

{% if cte_definitions | length > 0 %}
with
  {{ cte_definitions | join(",\n") }}
{% endif %}
select *
from (
  {{ union_queries | join(" union ") }}
) as final_result