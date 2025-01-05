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
        coalesce(prov.billing_provider_fein, prov.rendering_bill_provider_fein, prov.referring_provider_fein) as provider_fein,
    from {{ ref('stg_pharmacy_header') }} hdr
    left join {{ ref('dim_employer') }} employer
        on hdr.employer_fein = employer.employer_fein
    left join {{ ref('dim_insurer') }} ins
        on hdr.insurer_fein = ins.insurer_fein
    left join {{ ref('dim_facility') }} fac
        on hdr.facility_fein = fac.facility_fein
    left join {{ ref('dim_provider') }} prov
        on hdr.billing_provider_fein = prov.billing_provider_fein
    left join {{ ref('dim_provider') }} prov
        on hdr.rendering_bill_provider_fein = prov.rendering_bill_provider_fein
    left join {{ ref('dim_provider') }} prov
        on hdr.referring_provider_fein = prov.referring_provider_fein
    left join {{ ref('dim_employee') }} employee
        on hdr.patient_account_number = employee.patient_account_number
)

select
    bill_id,
    unique_bill_id_number,
    claim_administrator_claim,
    bill_type,
    billing_type_code,
    place_of_service_bill_code,
    billing_format_code,
    provider_signature_on_file,
    release_of_information_code,
    provider_agreement_code,
    bill_submission_reason_code,
    total_charge_per_bill,
    total_amount_paid_per_bill,
    patient_account_number,
    transaction_tracking_number,
    managed_care_organization,
    contract_type_code,
    treatment_authorization_number,
    bill_selection_date_key,
    bill_selection_date,
    date_insurer_received_bill,
    service_bill_from_date,
    service_bill_to_date,
    date_of_bill,
    date_insurer_paid_bill,
    employee_date_of_injury,
    employer_fein,
    insurer_fein,
    facility_fein,
    provider_fein
from base