{% set exists_i_current = check_table_exists('raw', 'institutional_header_current') %}
{% set exists_i_historical = check_table_exists('raw', 'institutional_header_historical') %}
{% set exists_pr_current = check_table_exists('raw', 'professional_header_current') %}
{% set exists_pr_historical = check_table_exists('raw', 'professional_header_historical') %}
{% set exists_ph_current = check_table_exists('raw', 'pharmacy_header_current') %}
{% set exists_ph_historical = check_table_exists('raw', 'pharmacy_header_historical') %}

with
{% if exists_i_current %}
institutional_header_current as (
    select distinct
        cast(bill_id as varchar) as visit_occurrence_id,
        case 
            when patient_account_number is null 
                or trim(patient_account_number) = '' 
            then lpad(
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
        cast(null as integer) as visit_concept_id,
        cast(service_bill_from_date as date) as visit_start_date,
        cast(service_bill_from_date as timestamp) as visit_start_datetime,
        cast(service_bill_to_date as date) as visit_end_date,
        cast(service_bill_to_date as timestamp) as visit_end_datetime,
        32855 as visit_type_concept_id,
        cast(null as integer) as provider_id,
        cast(null as integer) as care_site_id,
        cast(facility_code as varchar) as visit_source_value,
        cast(null as integer) as visit_source_concept_id,
        cast(null as integer) as admitted_from_source_value,
        cast(null as integer) as discharged_to_concept_id,
        cast(null as integer) as discharged_to_source_value,
        cast(null as integer) as preceding_visit_occurrence_id
    from {{ source('raw', 'institutional_header_current') }}
)
{% endif %}

{% if exists_i_historical %}
{% if exists_i_current %}, {% endif %}
institutional_header_historical as (
    select distinct
        cast(bill_id as varchar) as visit_occurrence_id,
        case 
            when patient_account_number is null 
                or trim(patient_account_number) = '' 
            then lpad(
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
        cast(null as integer) as visit_concept_id,
        cast(service_bill_from_date as date) as visit_start_date,
        cast(service_bill_from_date as timestamp) as visit_start_datetime,
        cast(service_bill_to_date as date) as visit_end_date,
        cast(service_bill_to_date as timestamp) as visit_end_datetime,
        32855 as visit_type_concept_id,
        cast(null as integer) as provider_id,
        cast(null as integer) as care_site_id,
        cast(facility_code as varchar) as visit_source_value,
        cast(null as integer) as visit_source_concept_id,
        cast(null as integer) as admitted_from_source_value,
        cast(null as integer) as discharged_to_concept_id,
        cast(null as integer) as discharged_to_source_value,
        cast(null as integer) as preceding_visit_occurrence_id
    from {{ source('raw', 'institutional_header_historical') }}
)
{% endif %}

{% if exists_pr_historical %}
{% if exists_i_historical %}, {% endif %}
professional_header_historical as (
    select distinct
        cast(bill_id as varchar) as visit_occurrence_id,
        case 
            when patient_account_number is null 
                or trim(patient_account_number) = '' 
            then lpad(
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
        cast(null as integer) as visit_concept_id,
        cast(service_bill_from_date as date) as visit_start_date,
        cast(service_bill_from_date as timestamp) as visit_start_datetime,
        cast(service_bill_to_date as date) as visit_end_date,
        cast(service_bill_to_date as timestamp) as visit_end_datetime,
        32873 as visit_type_concept_id,
        cast(null as integer) as provider_id,
        cast(null as integer) as care_site_id,
        cast(place_of_service_bill_code as varchar) as visit_source_value,
        cast(null as integer) as visit_source_concept_id,
        cast(null as integer) as admitted_from_source_value,
        cast(null as integer) as discharged_to_concept_id,
        cast(null as integer) as discharged_to_source_value,
        cast(null as integer) as preceding_visit_occurrence_id
    from {{ source('raw', 'professional_header_historical') }}
)
{% endif %}

{% if exists_pr_current %}
{% if exists_pr_historical %}, {% endif %}
professional_header_current as (
    select distinct
        cast(bill_id as varchar) as visit_occurrence_id,
        case 
            when patient_account_number is null 
                or trim(patient_account_number) = '' 
            then lpad(
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
        cast(null as integer) as visit_concept_id,
        cast(service_bill_from_date as date) as visit_start_date,
        cast(service_bill_from_date as timestamp) as visit_start_datetime,
        cast(service_bill_to_date as date) as visit_end_date,
        cast(service_bill_to_date as timestamp) as visit_end_datetime,
        32873 as visit_type_concept_id,
        cast(null as integer) as provider_id,
        cast(null as integer) as care_site_id,
        cast(place_of_service_bill_code as varchar) as visit_source_value,
        cast(null as integer) as visit_source_concept_id,
        cast(null as integer) as admitted_from_source_value,
        cast(null as integer) as discharged_to_concept_id,
        cast(null as integer) as discharged_to_source_value,
        cast(null as integer) as preceding_visit_occurrence_id
    from {{ source('raw', 'professional_header_current') }}
)
{% endif %}

{% if exists_ph_current %}
{% if exists_pr_current %}, {% endif %}
pharmacy_header_current as (
    select distinct
        cast(bill_id as varchar) as visit_occurrence_id,
        case 
            when patient_account_number is null 
                or trim(patient_account_number) = '' 
            then lpad(
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
        cast(null as integer) as visit_concept_id,
        cast(service_bill_from_date as date) as visit_start_date,
        cast(service_bill_from_date as timestamp) as visit_start_datetime,
        cast(service_bill_to_date as date) as visit_end_date,
        cast(service_bill_to_date as timestamp) as visit_end_datetime,
        32869 as visit_type_concept_id,
        cast(null as integer) as provider_id,
        cast(null as integer) as care_site_id,
        cast(place_of_service_bill_code as varchar) as visit_source_value,
        cast(null as integer) as visit_source_concept_id,
        cast(null as integer) as admitted_from_source_value,
        cast(null as integer) as discharged_to_concept_id,
        cast(null as integer) as discharged_to_source_value,
        cast(null as integer) as preceding_visit_occurrence_id
    from {{ source('raw', 'pharmacy_header_current') }}
)
{% endif %}

{% if exists_ph_historical %}
{% if exists_ph_current %}, {% endif %}
pharmacy_header_historical as (
    select distinct
        cast(bill_id as varchar) as visit_occurrence_id,
        case 
            when patient_account_number is null 
                or trim(patient_account_number) = '' 
            then lpad(
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
        cast(null as integer) as visit_concept_id,
        cast(service_bill_from_date as date) as visit_start_date,
        cast(service_bill_from_date as timestamp) as visit_start_datetime,
        cast(service_bill_to_date as date) as visit_end_date,
        cast(service_bill_to_date as timestamp) as visit_end_datetime,
        32869 as visit_type_concept_id,
        cast(null as integer) as provider_id,
        cast(null as integer) as care_site_id,
        cast(place_of_service_bill_code as varchar) as visit_source_value,
        cast(null as integer) as visit_source_concept_id,
        cast(null as integer) as admitted_from_source_value,
        cast(null as integer) as discharged_to_concept_id,
        cast(null as integer) as discharged_to_source_value,
        cast(null as integer) as preceding_visit_occurrence_id
    from {{ source('raw', 'pharmacy_header_historical') }}
)
{% endif %}

{% set cte_list = [] %}
{% if exists_i_current %}
  {% set _ = cte_list.append("select * from institutional_header_current") %}
{% endif %}
{% if exists_i_historical %}
  {% set _ = cte_list.append("select * from institutional_header_historical") %}
{% endif %}
{% if exists_pr_current %}
  {% set _ = cte_list.append("select * from professional_header_current") %}
{% endif %}
{% if exists_pr_historical %}
  {% set _ = cte_list.append("select * from professional_header_historical") %}
{% endif %}
{% if exists_ph_current %}
  {% set _ = cte_list.append("select * from pharmacy_header_current") %}
{% endif %}
{% if exists_ph_historical %}
  {% set _ = cte_list.append("select * from pharmacy_header_historical") %}
{% endif %}

select *
from (
    {{ cte_list | join(" union ") }}
) as final_result