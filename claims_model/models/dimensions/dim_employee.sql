with base as (
    select
        patient_account_number,
        employee_mailing_city,
        employee_mailing_state_code,
        employee_mailing_postal_code,
        employee_mailing_country,
        employee_date_of_birth,
        employee_gender_code,
        employee_marital_status_code
    from {{ ref('stg_pharmacy_header') }}
    where patient_account_number is not null

    union

    select
        patient_account_number,
        employee_mailing_city,
        employee_mailing_state_code,
        employee_mailing_postal_code,
        employee_mailing_country,
        employee_date_of_birth,
        employee_gender_code,
        employee_marital_status_code
    from {{ ref('stg_institutional_header') }}
    where patient_account_number is not null

    union

    select
        patient_account_number,
        employee_mailing_city,
        employee_mailing_state_code,
        employee_mailing_postal_code,
        employee_mailing_country,
        employee_date_of_birth,
        employee_gender_code,
        employee_marital_status_code
    from {{ ref('stg_professional_header') }}
    where patient_account_number is not null
)

select distinct
    patient_account_number,
    employee_mailing_city,
    employee_mailing_state_code,
    employee_mailing_postal_code,
    employee_mailing_country,
    employee_date_of_birth,
    employee_gender_code,
    employee_marital_status_code
from base