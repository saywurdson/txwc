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
        cast(null as integer) as gender_concept_id,
        extract(year from cast(employee_date_of_birth as date)) as year_of_birth,
        extract(month from cast(employee_date_of_birth as date)) as month_of_birth,
        extract(day from cast(employee_date_of_birth as date)) as day_of_birth,
        cast(employee_date_of_birth as timestamp) as birth_datetime,
        cast(null as integer) as race_concept_id,
        cast(null as integer) as ethnicity_concept_id,
        cast(null as integer) as location_id,
        cast(null as integer) as provider_id,
        cast(null as integer) as care_site_id,
        cast(patient_account_number as varchar) as person_source_value,
        cast(employee_gender_code as varchar) as gender_source_value,
        cast(null as integer) as gender_source_concept_id,
        cast(null as varchar) as race_source_value,
        cast(null as integer) as race_source_concept_id,
        cast(null as varchar) as ethnicity_source_value,
        cast(null as integer) as ethnicity_source_concept_id
    from {{ source('raw', 'institutional_header_current') }}
)
{% endif %}

{% if exists_i_historical %}
{% if exists_i_current %}, {% endif %}
institutional_header_historical as (
    select distinct
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
        cast(null as integer) as gender_concept_id,
        extract(year from cast(employee_date_of_birth as date)) as year_of_birth,
        extract(month from cast(employee_date_of_birth as date)) as month_of_birth,
        extract(day from cast(employee_date_of_birth as date)) as day_of_birth,
        cast(employee_date_of_birth as timestamp) as birth_datetime,
        cast(null as integer) as race_concept_id,
        cast(null as integer) as ethnicity_concept_id,
        cast(null as integer) as location_id,
        cast(null as integer) as provider_id,
        cast(null as integer) as care_site_id,
        cast(patient_account_number as varchar) as person_source_value,
        cast(employee_gender_code as varchar) as gender_source_value,
        cast(null as integer) as gender_source_concept_id,
        cast(null as varchar) as race_source_value,
        cast(null as integer) as race_source_concept_id,
        cast(null as varchar) as ethnicity_source_value,
        cast(null as integer) as ethnicity_source_concept_id
    from {{ source('raw', 'institutional_header_historical') }}
)
{% endif %}

{% if exists_pr_historical %}
{% if exists_i_historical %}, {% endif %}
professional_header_historical as (
    select distinct
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
        cast(null as integer) as gender_concept_id,
        extract(year from cast(employee_date_of_birth as date)) as year_of_birth,
        extract(month from cast(employee_date_of_birth as date)) as month_of_birth,
        extract(day from cast(employee_date_of_birth as date)) as day_of_birth,
        cast(employee_date_of_birth as timestamp) as birth_datetime,
        cast(null as integer) as race_concept_id,
        cast(null as integer) as ethnicity_concept_id,
        cast(null as integer) as location_id,
        cast(null as integer) as provider_id,
        cast(null as integer) as care_site_id,
        cast(patient_account_number as varchar) as person_source_value,
        cast(employee_gender_code as varchar) as gender_source_value,
        cast(null as integer) as gender_source_concept_id,
        cast(null as varchar) as race_source_value,
        cast(null as integer) as race_source_concept_id,
        cast(null as varchar) as ethnicity_source_value,
        cast(null as integer) as ethnicity_source_concept_id
    from {{ source('raw', 'professional_header_historical') }}
)
{% endif %}

{% if exists_pr_current %}
{% if exists_pr_historical %}, {% endif %}
professional_header_current as (
    select distinct
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
        cast(null as integer) as gender_concept_id,
        extract(year from cast(employee_date_of_birth as date)) as year_of_birth,
        extract(month from cast(employee_date_of_birth as date)) as month_of_birth,
        extract(day from cast(employee_date_of_birth as date)) as day_of_birth,
        cast(employee_date_of_birth as timestamp) as birth_datetime,
        cast(null as integer) as race_concept_id,
        cast(null as integer) as ethnicity_concept_id,
        cast(null as integer) as location_id,
        cast(null as integer) as provider_id,
        cast(null as integer) as care_site_id,
        cast(patient_account_number as varchar) as person_source_value,
        cast(employee_gender_code as varchar) as gender_source_value,
        cast(null as integer) as gender_source_concept_id,
        cast(null as varchar) as race_source_value,
        cast(null as integer) as race_source_concept_id,
        cast(null as varchar) as ethnicity_source_value,
        cast(null as integer) as ethnicity_source_concept_id
    from {{ source('raw', 'professional_header_current') }}
)
{% endif %}

{% if exists_ph_current %}
{% if exists_pr_current %}, {% endif %}
pharmacy_header_current as (
    select distinct
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
        cast(null as integer) as gender_concept_id,
        extract(year from cast(employee_date_of_birth as date)) as year_of_birth,
        extract(month from cast(employee_date_of_birth as date)) as month_of_birth,
        extract(day from cast(employee_date_of_birth as date)) as day_of_birth,
        cast(employee_date_of_birth as timestamp) as birth_datetime,
        cast(null as integer) as race_concept_id,
        cast(null as integer) as ethnicity_concept_id,
        cast(null as integer) as location_id,
        cast(null as integer) as provider_id,
        cast(null as integer) as care_site_id,
        cast(patient_account_number as varchar) as person_source_value,
        cast(employee_gender_code as varchar) as gender_source_value,
        cast(null as integer) as gender_source_concept_id,
        cast(null as varchar) as race_source_value,
        cast(null as integer) as race_source_concept_id,
        cast(null as varchar) as ethnicity_source_value,
        cast(null as integer) as ethnicity_source_concept_id
    from {{ source('raw', 'pharmacy_header_current') }}
)
{% endif %}

{% if exists_ph_historical %}
{% if exists_ph_current %}, {% endif %}
pharmacy_header_historical as (
    select distinct
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
        cast(null as integer) as gender_concept_id,
        extract(year from cast(employee_date_of_birth as date)) as year_of_birth,
        extract(month from cast(employee_date_of_birth as date)) as month_of_birth,
        extract(day from cast(employee_date_of_birth as date)) as day_of_birth,
        cast(employee_date_of_birth as timestamp) as birth_datetime,
        cast(null as integer) as race_concept_id,
        cast(null as integer) as ethnicity_concept_id,
        cast(null as integer) as location_id,
        cast(null as integer) as provider_id,
        cast(null as integer) as care_site_id,
        cast(patient_account_number as varchar) as person_source_value,
        cast(employee_gender_code as varchar) as gender_source_value,
        cast(null as integer) as gender_source_concept_id,
        cast(null as varchar) as race_source_value,
        cast(null as integer) as race_source_concept_id,
        cast(null as varchar) as ethnicity_source_value,
        cast(null as integer) as ethnicity_source_concept_id
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