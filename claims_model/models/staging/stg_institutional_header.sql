{% set exists_current = check_table_exists('raw', 'institutional_header_current') %}
{% set exists_historical = check_table_exists('raw', 'institutional_header_historical') %}

with
{% if exists_current %}
institutional_header_current as(
    select
        bill_selection_date,
        bill_id,
        billing_provider_unique_bill,
        unique_bill_id_number,
        bill_type,
        reporting_period_start_date,
        reporting_period_end_date,
        case
            when insurer_fein is null 
                or trim(insurer_fein) = '' 
            then lpad(
                cast(
                    (
                    hash(
                        concat_ws(
                        '||',
                        coalesce(insurer_postal_code, ''),
                        coalesce(claim_administrator_fein, ''),
                        coalesce(claim_administrator_name, ''),
                        coalesce(claim_administrator_postal, '')
                        ),
                        'xxhash64'
                    ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
                )
            else insurer_fein
        end as insurer_fein,
        insurer_postal_code,
        claim_administrator_fein,
        claim_administrator_name,
        claim_administrator_postal,
        transaction_set_purpose_code,
        case
            when employer_fein is null 
                or trim(employer_fein) = '' 
            then lpad(
                cast(
                    (
                    hash(
                        concat_ws(
                        '||',
                        coalesce(employer_physical_city, ''),
                        coalesce(employer_physical_state_code, ''),
                        coalesce(employer_physical_postal, ''),
                        coalesce(employer_physical_country, '')
                        ),
                        'xxhash64'
                    ) % 1000000000
                    ) as varchar
                ), 
                9, 
                '0'
                )
            else employer_fein
        end as employer_fein,
        employer_physical_city,
        employer_physical_state_code,
        employer_physical_postal,
        employer_physical_country,
        employee_mailing_city,
        employee_mailing_state_code,
        employee_mailing_postal_code,
        employee_mailing_country,
        employee_date_of_birth,
        employee_gender_code,
        employee_marital_status_code,
        claim_administrator_claim,
        employee_date_of_injury,
        total_charge_per_bill,
        billing_type_code,
        place_of_service_bill_code,
        billing_format_code,
        provider_signature_on_file,
        release_of_information_code,
        provider_agreement_code,
        facility_code,
        bill_frequency_type_code,
        admission_date,
        admission_hour,
        discharge_date,
        discharge_hour,
        admission_type_code,
        diagnosis_related_group_code,
        bill_submission_reason_code,
        date_insurer_received_bill,
        service_bill_from_date,
        service_bill_to_date,
        date_of_bill,
        date_insurer_paid_bill,
        contract_type_code,
        total_amount_paid_per_bill,
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
        end as patient_account_number,
        transaction_tracking_number,
        first_icd_9cm_or_icd_10cm,
        second_icd_9cm_or_icd_10cm,
        third_icd_9cm_or_icd_10cm,
        fourth_icd_9cm_or_icd_10cm,
        fifth_icd_9cm_or_icd_10cm,
        principal_diagnosis_code,
        admitting_diagnosis_code,
        icd_9cm_or_icd_10cm_principal,
        principal_procedure_date,
        first_icd_9cm_or_icd_10cm_1,
        second_icd_9cm_or_icd_10cm_1,
        third_icd_9cm_or_icd_10cm_1,
        fourth_icd_9cm_or_icd_10cm_1,
        first_procedure_date,
        second_procedure_date,
        third_procedure_date,
        fourth_procedure_date,
        facility_name,
        case
            when facility_fein is null 
                or trim(facility_fein) = '' 
            then lpad(
                cast(
                    (
                    hash(
                        concat_ws(
                        '||',
                        coalesce(facility_name, ''),
                        coalesce(facility_primary_address, ''),
                        coalesce(facility_secondary_address, ''),
                        coalesce(facility_city, ''),
                        coalesce(facility_state_code, ''),
                        coalesce(facility_postal_code, ''),
                        coalesce(facility_country_code, ''),
                        coalesce(facility_state_license_number, ''),
                        coalesce(facility_medicare_number, ''),
                        coalesce(facility_national_provider, '')
                        ),
                        'xxhash64'
                    ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
                )
            else facility_fein
        end as facility_fein,
        facility_primary_address,
        facility_secondary_address,
        facility_city,
        facility_state_code,
        facility_postal_code,
        facility_country_code,
        facility_state_license_number,
        facility_medicare_number,
        facility_national_provider,
        managed_care_organization,
        billing_provider_last_name,
        billing_provider_first_name,
        billing_provider_middle_name,
        billing_provider_suffix,
        case
            when billing_provider_fein is null 
                or trim(billing_provider_fein) = '' 
            then lpad(
                cast(
                    (
                    hash(
                        concat_ws(
                        '||',
                        coalesce(billing_provider_last_name, ''),
                        coalesce(billing_provider_first_name, ''),
                        coalesce(billing_provider_middle_name, ''),
                        coalesce(billing_provider_suffix, ''),
                        coalesce(billing_provider_gate_keeper, ''),
                        coalesce(billing_provider_primary, ''),
                        coalesce(billing_provider_primary_1, ''),
                        coalesce(billing_provider_secondary, ''),
                        coalesce(billing_provider_city, ''),
                        coalesce(billing_provider_state_code, ''),
                        coalesce(billing_provider_postal_code, ''),
                        coalesce(billing_provider_country, ''),
                        coalesce(billing_provider_state_license, ''),
                        coalesce(billing_provider_medicare, ''),
                        coalesce(treatment_authorization_number, ''),
                        coalesce(billing_provider_national, '')
                        ),
                        'xxhash64'
                    ) % 1000000000
                    ) as varchar
                ), 
                9, 
                '0'
                )
            else billing_provider_fein
        end as billing_provider_fein,
        billing_provider_gate_keeper,
        billing_provider_primary,
        billing_provider_primary_1,
        billing_provider_secondary,
        billing_provider_city,
        billing_provider_state_code,
        billing_provider_postal_code,
        billing_provider_country,
        billing_provider_state_license,
        billing_provider_medicare,
        treatment_authorization_number,
        billing_provider_national,
        rendering_bill_provider_last,
        rendering_bill_provider_first,
        rendering_bill_provider_middle,
        rendering_bill_provider_suffix,
        case
            when rendering_bill_provider_fein is null 
                or trim(rendering_bill_provider_fein) = '' 
            then lpad(
                cast(
                    (
                    hash(
                        concat_ws(
                        '||',
                        coalesce(rendering_bill_provider_last, ''),
                        coalesce(rendering_bill_provider_first, ''),
                        coalesce(rendering_bill_provider_middle, ''),
                        coalesce(rendering_bill_provider_suffix, ''),
                        coalesce(rendering_bill_provider_gate, ''),
                        coalesce(rendering_bill_provider, ''),
                        coalesce(rendering_bill_provider_1, ''),
                        coalesce(rendering_bill_provider_2, ''),
                        coalesce(rendering_bill_provider_city, ''),
                        coalesce(rendering_bill_provider_state, ''),
                        coalesce(rendering_bill_provider_postal, ''),
                        coalesce(rendering_bill_provider_3, ''),
                        coalesce(rendering_bill_provider_state_1, ''),
                        coalesce(rendering_bill_provider_4, '')
                        ),
                        'xxhash64'
                    ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
                )
            else rendering_bill_provider_fein
        end as rendering_bill_provider_fein,
        rendering_bill_provider_gate,
        rendering_bill_provider,
        rendering_bill_provider_1,
        rendering_bill_provider_2,
        rendering_bill_provider_city,
        rendering_bill_provider_state,
        rendering_bill_provider_postal,
        rendering_bill_provider_3,
        rendering_bill_provider_state_1,
        rendering_bill_provider_4,
        referring_provider_last_name,
        referring_provider_first,
        referring_provider_middle,
        referring_provider_suffix,
        case
            when referring_provider_fein is null 
                or trim(referring_provider_fein) = '' 
            then lpad(
                cast(
                    (
                    hash(
                        concat_ws(
                        '||',
                        coalesce(referring_provider_last_name, ''),
                        coalesce(referring_provider_first, ''),
                        coalesce(referring_provider_middle, ''),
                        coalesce(referring_provider_suffix, ''),
                        coalesce(referring_provider_gate_keeper, ''),
                        coalesce(referring_provider_state, ''),
                        coalesce(referring_provider_specialty, ''),
                        coalesce(referring_provider_medicare, ''),
                        coalesce(referring_provider_national, '')
                        ),
                        'xxhash64'
                    ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
                )
            else referring_provider_fein
        end as referring_provider_fein,
        referring_provider_gate_keeper,
        referring_provider_state,
        referring_provider_specialty,
        referring_provider_medicare,
        referring_provider_national
    from {{ source('raw', 'institutional_header_current') }}
)
{% endif %}

{% if exists_historical %}
{% if exists_current %}, {% endif %}
institutional_header_historical as (
    select
        bill_selection_date,
        bill_id,
        billing_provider_unique_bill,
        unique_bill_id_number,
        bill_type,
        reporting_period_start_date,
        reporting_period_end_date,
        case
            when insurer_fein is null 
                or trim(insurer_fein) = '' 
            then lpad(
                cast(
                    (
                    hash(
                        concat_ws(
                        '||',
                        coalesce(insurer_postal_code, ''),
                        coalesce(claim_administrator_fein, ''),
                        coalesce(claim_administrator_name, ''),
                        coalesce(claim_administrator_postal, '')
                        ),
                        'xxhash64'
                    ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
                )
            else insurer_fein
        end as insurer_fein,
        insurer_postal_code,
        claim_administrator_fein,
        claim_administrator_name,
        claim_administrator_postal,
        transaction_set_purpose_code,
        case
            when employer_fein is null 
                or trim(employer_fein) = '' 
            then lpad(
                cast(
                    (
                    hash(
                        concat_ws(
                        '||',
                        coalesce(employer_physical_city, ''),
                        coalesce(employer_physical_state_code, ''),
                        coalesce(employer_physical_postal, ''),
                        coalesce(employer_physical_country, '')
                        ),
                        'xxhash64'
                    ) % 1000000000
                    ) as varchar
                ), 
                9, 
                '0'
                )
            else employer_fein
        end as employer_fein,
        employer_physical_city,
        employer_physical_state_code,
        employer_physical_postal,
        employer_physical_country,
        employee_mailing_city,
        employee_mailing_state_code,
        employee_mailing_postal_code,
        employee_mailing_country,
        employee_date_of_birth,
        employee_gender_code,
        employee_marital_status_code,
        claim_administrator_claim,
        employee_date_of_injury,
        total_charge_per_bill,
        billing_type_code,
        place_of_service_bill_code,
        billing_format_code,
        provider_signature_on_file,
        release_of_information_code,
        provider_agreement_code,
        facility_code,
        bill_frequency_type_code,
        admission_date,
        admission_hour,
        discharge_date,
        discharge_hour,
        admission_type_code,
        diagnosis_related_group_code,
        bill_submission_reason_code,
        date_insurer_received_bill,
        service_bill_from_date,
        service_bill_to_date,
        date_of_bill,
        date_insurer_paid_bill,
        contract_type_code,
        total_amount_paid_per_bill,
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
        end as patient_account_number,
        transaction_tracking_number,
        first_icd_9cm_or_icd_10cm,
        second_icd_9cm_or_icd_10cm,
        third_icd_9cm_or_icd_10cm,
        fourth_icd_9cm_or_icd_10cm,
        fifth_icd_9cm_or_icd_10cm,
        principal_diagnosis_code,
        admitting_diagnosis_code,
        icd_9cm_or_icd_10cm_principal,
        principal_procedure_date,
        first_icd_9cm_or_icd_10cm_1,
        second_icd_9cm_or_icd_10cm_1,
        third_icd_9cm_or_icd_10cm_1,
        fourth_icd_9cm_or_icd_10cm_1,
        first_procedure_date,
        second_procedure_date,
        third_procedure_date,
        fourth_procedure_date,
        facility_name,
        case
            when facility_fein is null 
                or trim(facility_fein) = '' 
            then lpad(
                cast(
                    (
                    hash(
                        concat_ws(
                        '||',
                        coalesce(facility_name, ''),
                        coalesce(facility_primary_address, ''),
                        coalesce(facility_secondary_address, ''),
                        coalesce(facility_city, ''),
                        coalesce(facility_state_code, ''),
                        coalesce(facility_postal_code, ''),
                        coalesce(facility_country_code, ''),
                        coalesce(facility_state_license_number, ''),
                        coalesce(facility_medicare_number, ''),
                        coalesce(facility_national_provider, '')
                        ),
                        'xxhash64'
                    ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
                )
            else facility_fein
        end as facility_fein,
        facility_primary_address,
        facility_secondary_address,
        facility_city,
        facility_state_code,
        facility_postal_code,
        facility_country_code,
        facility_state_license_number,
        facility_medicare_number,
        facility_national_provider,
        managed_care_organization,
        billing_provider_last_name,
        billing_provider_first_name,
        billing_provider_middle_name,
        billing_provider_suffix,
        case
            when billing_provider_fein is null 
                or trim(billing_provider_fein) = '' 
            then lpad(
                cast(
                    (
                    hash(
                        concat_ws(
                        '||',
                        coalesce(billing_provider_last_name, ''),
                        coalesce(billing_provider_first_name, ''),
                        coalesce(billing_provider_middle_name, ''),
                        coalesce(billing_provider_suffix, ''),
                        coalesce(billing_provider_gate_keeper, ''),
                        coalesce(billing_provider_primary, ''),
                        coalesce(billing_provider_primary_1, ''),
                        coalesce(billing_provider_secondary, ''),
                        coalesce(billing_provider_city, ''),
                        coalesce(billing_provider_state_code, ''),
                        coalesce(billing_provider_postal_code, ''),
                        coalesce(billing_provider_country, ''),
                        coalesce(billing_provider_state_license, ''),
                        coalesce(billing_provider_medicare, ''),
                        coalesce(treatment_authorization_number, ''),
                        coalesce(billing_provider_national, '')
                        ),
                        'xxhash64'
                    ) % 1000000000
                    ) as varchar
                ), 
                9, 
                '0'
                )
            else billing_provider_fein
        end as billing_provider_fein,
        billing_provider_gate_keeper,
        billing_provider_primary,
        billing_provider_primary_1,
        billing_provider_secondary,
        billing_provider_city,
        billing_provider_state_code,
        billing_provider_postal_code,
        billing_provider_country,
        billing_provider_state_license,
        billing_provider_medicare,
        treatment_authorization_number,
        billing_provider_national,
        rendering_bill_provider_last,
        rendering_bill_provider_first,
        rendering_bill_provider_middle,
        rendering_bill_provider_suffix,
        case
            when rendering_bill_provider_fein is null 
                or trim(rendering_bill_provider_fein) = '' 
            then lpad(
                cast(
                    (
                    hash(
                        concat_ws(
                        '||',
                        coalesce(rendering_bill_provider_last, ''),
                        coalesce(rendering_bill_provider_first, ''),
                        coalesce(rendering_bill_provider_middle, ''),
                        coalesce(rendering_bill_provider_suffix, ''),
                        coalesce(rendering_bill_provider_gate, ''),
                        coalesce(rendering_bill_provider, ''),
                        coalesce(rendering_bill_provider_1, ''),
                        coalesce(rendering_bill_provider_2, ''),
                        coalesce(rendering_bill_provider_city, ''),
                        coalesce(rendering_bill_provider_state, ''),
                        coalesce(rendering_bill_provider_postal, ''),
                        coalesce(rendering_bill_provider_3, ''),
                        coalesce(rendering_bill_provider_state_1, ''),
                        coalesce(rendering_bill_provider_4, '')
                        ),
                        'xxhash64'
                    ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
                )
            else rendering_bill_provider_fein
        end as rendering_bill_provider_fein,
        rendering_bill_provider_gate,
        rendering_bill_provider,
        rendering_bill_provider_1,
        rendering_bill_provider_2,
        rendering_bill_provider_city,
        rendering_bill_provider_state,
        rendering_bill_provider_postal,
        rendering_bill_provider_3,
        rendering_bill_provider_state_1,
        rendering_bill_provider_4,
        referring_provider_last_name,
        referring_provider_first,
        referring_provider_middle,
        referring_provider_suffix,
        case
            when referring_provider_fein is null 
                or trim(referring_provider_fein) = '' 
            then lpad(
                cast(
                    (
                    hash(
                        concat_ws(
                        '||',
                        coalesce(referring_provider_last_name, ''),
                        coalesce(referring_provider_first, ''),
                        coalesce(referring_provider_middle, ''),
                        coalesce(referring_provider_suffix, ''),
                        coalesce(referring_provider_gate_keeper, ''),
                        coalesce(referring_provider_state, ''),
                        coalesce(referring_provider_specialty, ''),
                        coalesce(referring_provider_medicare, ''),
                        coalesce(referring_provider_national, '')
                        ),
                        'xxhash64'
                    ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
                )
            else referring_provider_fein
        end as referring_provider_fein,
        referring_provider_gate_keeper,
        referring_provider_state,
        referring_provider_specialty,
        referring_provider_medicare,
        referring_provider_national
    from {{ source('raw', 'institutional_header_historical') }}
)
{% endif %}

select * 
from
    {% if exists_current and exists_historical %}
            institutional_header_current
            union
            select * from institutional_header_historical
        {% elif exists_current %}
            institutional_header_current
        {% elif exists_historical %}
            institutional_header_historical
        {% endif %}