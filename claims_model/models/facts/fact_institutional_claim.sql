with base as (
    select
        hdr.bill_id,
        hdr.unique_bill_id_number,
        hdr.claim_administrator_claim,
        hdr.bill_type,
        hdr.billing_type_code,
        hdr.place_of_service_bill_code,
        hdr.billing_format_code,
        hdr.provider_signature_on_file,
        hdr.release_of_information_code,
        hdr.provider_agreement_code,
        hdr.facility_code,
        hdr.bill_frequency_type_code,
        hdr.admission_date,
        hdr.admission_hour,
        hdr.discharge_date,
        hdr.discharge_hour,
        hdr.admission_type_code,
        hdr.diagnosis_related_group_code,
        hdr.bill_submission_reason_code,
        hdr.total_charge_per_bill,
        hdr.total_amount_paid_per_bill,
        employee.patient_account_number,
        hdr.transaction_tracking_number,
        hdr.managed_care_organization,
        hdr.contract_type_code,
        hdr.treatment_authorization_number,
        hdr.bill_selection_date,
        hdr.date_insurer_received_bill,
        hdr.service_bill_from_date,
        hdr.service_bill_to_date,
        hdr.date_of_bill,
        hdr.date_insurer_paid_bill,
        hdr.employee_date_of_injury,
        emp.employer_fein,
        ins.insurer_fein,
        fac.facility_fein,
        coalesce(prov.billing_provider_fein, prov.rendering_bill_provider_fein, prov.referring_provider_fein) as provider_fein
    from {{ ref('stg_institutional_header') }} hdr
    left join {{ ref('dim_employer') }} emp
        on hdr.employer_fein = emp.employer_fein
    left join {{ ref('dim_insurer') }} ins
        on hdr.insurer_fein = ins.insurer_fein
    left join {{ ref('dim_facility') }} fac
        on hdr.facility_fein = fac.facility_fein
    left join {{ ref('dim_provider') }} prov
        on hdr.billing_provider_fein = prov.billing_provider_fein
)

select * from base
