{% set exists_phd_current = check_table_exists('raw', 'pharmacy_detail_current') %}
{% set exists_phd_historical = check_table_exists('raw', 'pharmacy_detail_historical') %}
{% set exists_id_current = check_table_exists('raw', 'institutional_detail_current') %}
{% set exists_id_historical = check_table_exists('raw', 'institutional_detail_historical') %}
{% set exists_prd_current = check_table_exists('raw', 'professional_detail_current') %}
{% set exists_prd_historical = check_table_exists('raw', 'professional_detail_historical') %}

with
{% if exists_phd_current %}
-- get columns needed
pharmacy_detail_current as (
    select 
        cast(
            hash(
                concat_ws(
                    '||',
                    pdc.bill_id,
                    pdc.row_id
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as drug_exposure_id,
        case 
            when phc.patient_account_number is null or trim(phc.patient_account_number) = '' then lpad(
                cast(
                    (
                        hash(
                            concat_ws(
                                '||',
                                coalesce(phc.employee_mailing_city, ''),
                                coalesce(phc.employee_mailing_state_code, ''),
                                coalesce(phc.employee_mailing_postal_code, ''),
                                coalesce(phc.employee_mailing_country, ''),
                                coalesce(cast(phc.employee_date_of_birth as varchar), ''),
                                coalesce(phc.employee_gender_code, '')
                            ),
                            'xxhash64'
                        ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
            )
            else phc.patient_account_number
        end as person_id,
        cast(null as integer) as drug_concept_id,
        coalesce(cast(pdc.service_line_from_date as date), cast(pdc.prescription_line_date as date)) as drug_exposure_start_date,
        coalesce(cast(pdc.service_line_from_date as timestamp), cast(pdc.prescription_line_date as timestamp)) as drug_exposure_start_datetime,
        cast(pdc.service_line_from_date as date) + cast(pdc.drugs_supplies_number_of as integer) as drug_exposure_end_date,
        cast(pdc.service_line_from_date as timestamp) + (cast(pdc.drugs_supplies_number_of as integer) * INTERVAL '1' DAY) as drug_exposure_end_datetime,
        cast(pdc.service_line_to_date as date) as verbatim_end_date,
        32869 as drug_type_concept_id,
        cast(null as varchar) as stop_reason,
        0 as refills,
        cast(pdc.drugs_supplies_quantity as float) as quantity,
        cast(pdc.drugs_supplies_number_of as integer) as days_supply,
        cast(null as varchar) as sig,
        cast(null as integer) as route_concept_id,
        cast(null as integer) as lot_number,
        cast(
            hash(
                concat_ws(
                '||',
                phc.rendering_bill_provider_last,
                coalesce(phc.rendering_bill_provider_first, ''),
                coalesce(phc.rendering_bill_provider_middle, ''),
                phc.rendering_bill_provider_state_1,
                phc.rendering_bill_provider_4
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as provider_id,
        cast(pdc.bill_id as varchar) as visit_occurrence_id,
        cast(null as integer) as visit_detail_id,
        pdc.ndc_billed_code as drug_source_value,
        cast(null as integer) as drug_source_concept_id,
        cast(null as varchar) as route_source_value,
        cast(null as varchar) as dose_unit_source_value
    from {{ source('raw','pharmacy_detail_current') }} pdc 
    join {{ source('raw','pharmacy_header_current') }} phc
        on cast(pdc.bill_id as varchar) = cast(phc.bill_id as varchar)
)
{% endif %}

{% if exists_phd_historical %}
{% if exists_phd_current %}, {% endif %}
-- Get columns needed
pharmacy_detail_historical as (
    select 
        cast(
            hash(
                concat_ws(
                    '||',
                    pdh.bill_id,
                    pdh.row_id
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as drug_exposure_id,
        case 
            when phh.patient_account_number is null or trim(phh.patient_account_number) = '' then lpad(
                cast(
                    (
                        hash(
                            concat_ws(
                                '||',
                                coalesce(phh.employee_mailing_city, ''),
                                coalesce(phh.employee_mailing_state_code, ''),
                                coalesce(phh.employee_mailing_postal_code, ''),
                                coalesce(phh.employee_mailing_country, ''),
                                coalesce(cast(phh.employee_date_of_birth as varchar), ''),
                                coalesce(phh.employee_gender_code, '')
                            ),
                            'xxhash64'
                        ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
            )
            else phh.patient_account_number
        end as person_id,
        cast(null as integer) as drug_concept_id,
        coalesce(cast(pdh.service_line_from_date as date), cast(pdh.prescription_line_date as date)) as drug_exposure_start_date,
        coalesce(cast(pdh.service_line_from_date as timestamp), cast(pdh.prescription_line_date as timestamp)) as drug_exposure_start_datetime,
        cast(pdh.service_line_from_date as date) + cast(pdh.drugs_supplies_number_of as integer) as drug_exposure_end_date,
        cast(pdh.service_line_from_date as timestamp) + (cast(pdh.drugs_supplies_number_of as integer) * INTERVAL '1' DAY) as drug_exposure_end_datetime,
        cast(pdh.service_line_to_date as date) as verbatim_end_date,
        32869 as drug_type_concept_id,
        cast(null as varchar) as stop_reason,
        0 as refills,
        cast(pdh.drugs_supplies_quantity as float) as quantity,
        cast(pdh.drugs_supplies_number_of as integer) as days_supply,
        cast(null as varchar) as sig,
        cast(null as integer) as route_concept_id,
        cast(null as integer) as lot_number,
        cast(
            hash(
                concat_ws(
                '||',
                phh.rendering_bill_provider_last,
                coalesce(phh.rendering_bill_provider_first, ''),
                coalesce(phh.rendering_bill_provider_middle, ''),
                phh.rendering_bill_provider_state_1,
                phh.rendering_bill_provider_4
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as provider_id,
        cast(pdh.bill_id as varchar) as visit_occurrence_id,
        cast(null as integer) as visit_detail_id,
        pdh.ndc_billed_code as drug_source_value,
        cast(null as integer) as drug_source_concept_id,
        cast(null as varchar) as route_source_value,
        cast(null as varchar) as dose_unit_source_value
    from {{ source('raw','pharmacy_detail_historical') }} pdh
    join {{ source('raw','pharmacy_header_historical') }} phh
        on cast(pdh.bill_id as varchar) = cast(phh.bill_id as varchar)
)
{% endif %}

{% if exists_id_current %}
{% if exists_phd_historical %}, {% endif %}
institutional_detail_current as (
    select 
        cast(
            hash(
                concat_ws(
                    '||',
                    idc.bill_id,
                    idc.row_id
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as drug_exposure_id,
        case 
            when ihc.patient_account_number is null or trim(ihc.patient_account_number) = '' then lpad(
                cast(
                    (
                        hash(
                            concat_ws(
                                '||',
                                coalesce(ihc.employee_mailing_city, ''),
                                coalesce(ihc.employee_mailing_state_code, ''),
                                coalesce(ihc.employee_mailing_postal_code, ''),
                                coalesce(ihc.employee_mailing_country, ''),
                                coalesce(cast(ihc.employee_date_of_birth as varchar), ''),
                                coalesce(ihc.employee_gender_code, '')
                            ),
                            'xxhash64'
                        ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
            )
            else ihc.patient_account_number
        end as person_id,
        cast(null as integer) as drug_concept_id,
        cast(idc.service_line_from_date as date) as drug_exposure_start_date,
        cast(idc.service_line_from_date as timestamp) as drug_exposure_start_datetime,
        cast(idc.service_line_to_date as date) as drug_exposure_end_date,
        cast(idc.service_line_to_date as timestamp) as drug_exposure_end_datetime,
        cast(idc.service_line_to_date as date) as verbatim_end_date,
        32854 as drug_type_concept_id,
        cast(null as varchar) as stop_reason,
        0 as refills,
        cast(null as integer) as quantity,
        cast(idc.days_units_billed as integer) as days_supply,
        cast(null as varchar) as sig,
        cast(null as integer) as route_concept_id,
        cast(null as integer) as lot_number,
        cast(
            hash(
                concat_ws(
                '||',
                ihc.rendering_bill_provider_last,
                coalesce(ihc.rendering_bill_provider_first, ''),
                coalesce(ihc.rendering_bill_provider_middle, ''),
                ihc.rendering_bill_provider_state_1,
                ihc.rendering_bill_provider_4
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as provider_id,
        cast(idc.bill_id as varchar) as visit_occurrence_id,
        cast(null as integer) as visit_detail_id,
        idc.hcpcs_line_procedure_billed as drug_source_value,
        cast(null as integer) as drug_source_concept_id,
        cast(null as varchar) as route_source_value,
        cast(null as varchar) as dose_unit_source_value
    from {{ source('raw','institutional_detail_current') }} idc
    join {{ source('raw','institutional_header_current') }} ihc
        on cast(idc.bill_id as varchar) = cast(ihc.bill_id as varchar)
    join {{ source('terminology','concept') }} as c
        on c.concept_code = idc.hcpcs_line_procedure_billed
    where c.domain_id = 'Drug'
        and c.vocabulary_id = 'HCPCS'
)
{% endif %}

{% if exists_id_historical %}
{% if exists_id_current %}, {% endif %}
institutional_detail_historical as (
    select 
        cast(
            hash(
                concat_ws(
                    '||',
                    idh.bill_id,
                    idh.row_id
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as drug_exposure_id,
        case 
            when ihh.patient_account_number is null or trim(ihh.patient_account_number) = '' then lpad(
                cast(
                    (
                        hash(
                            concat_ws(
                                '||',
                                coalesce(ihh.employee_mailing_city, ''),
                                coalesce(ihh.employee_mailing_state_code, ''),
                                coalesce(ihh.employee_mailing_postal_code, ''),
                                coalesce(ihh.employee_mailing_country, ''),
                                coalesce(cast(ihh.employee_date_of_birth as varchar), ''),
                                coalesce(ihh.employee_gender_code, '')
                            ),
                            'xxhash64'
                        ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
            )
            else ihh.patient_account_number
        end as person_id,
        cast(null as integer) as drug_concept_id,
        cast(idh.service_line_from_date as date) as drug_exposure_start_date,
        cast(idh.service_line_from_date as timestamp) as drug_exposure_start_datetime,
        cast(idh.service_line_to_date as date) as drug_exposure_end_date,
        cast(idh.service_line_to_date as timestamp) as drug_exposure_end_datetime,
        cast(idh.service_line_to_date as date) as verbatim_end_date,
        32854 as drug_type_concept_id,
        cast(null as varchar) as stop_reason,
        0 as refills,
        cast(null as integer) as quantity,
        cast(idh.days_units_billed as integer) as days_supply,
        cast(null as varchar) as sig,
        cast(null as integer) as route_concept_id,
        cast(null as integer) as lot_number,
        cast(
            hash(
                concat_ws(
                '||',
                ihh.rendering_bill_provider_last,
                coalesce(ihh.rendering_bill_provider_first, ''),
                coalesce(ihh.rendering_bill_provider_middle, ''),
                ihh.rendering_bill_provider_state_1,
                ihh.rendering_bill_provider_4
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as provider_id,
        cast(idh.bill_id as varchar) as visit_occurrence_id,
        cast(null as integer) as visit_detail_id,
        idh.hcpcs_line_procedure_billed as drug_source_value,
        cast(null as integer) as drug_source_concept_id,
        cast(null as varchar) as route_source_value,
        cast(null as varchar) as dose_unit_source_value
    from {{ source('raw','institutional_detail_historical') }} idh
    join {{ source('raw','institutional_header_historical') }} ihh
        on cast(idh.bill_id as varchar) = cast(ihh.bill_id as varchar)
    join {{ source('terminology','concept') }} as c
        on c.concept_code = idh.hcpcs_line_procedure_billed
    where c.domain_id = 'Drug'
        and c.vocabulary_id = 'HCPCS'
)
{% endif %}

{% if exists_prd_current %}
{% if exists_id_historical %}, {% endif %}
professional_detail_current as (
    select 
        cast(
            hash(
                concat_ws(
                    '||',
                    prdc.bill_id,
                    prdc.row_id
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as drug_exposure_id,
        case 
            when prhc.patient_account_number is null or trim(prhc.patient_account_number) = '' then lpad(
                cast(
                    (
                        hash(
                            concat_ws(
                                '||',
                                coalesce(prhc.employee_mailing_city, ''),
                                coalesce(prhc.employee_mailing_state_code, ''),
                                coalesce(prhc.employee_mailing_postal_code, ''),
                                coalesce(prhc.employee_mailing_country, ''),
                                coalesce(cast(prhc.employee_date_of_birth as varchar), ''),
                                coalesce(prhc.employee_gender_code, '')
                            ),
                            'xxhash64'
                        ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
            )
            else prhc.patient_account_number
        end as person_id,
        cast(null as integer) as drug_concept_id,
        cast(prdc.service_line_from_date as date) as drug_exposure_start_date,
        cast(prdc.service_line_from_date as timestamp) as drug_exposure_start_datetime,
        cast(prdc.service_line_to_date as date) as drug_exposure_end_date,
        cast(prdc.service_line_to_date as timestamp) as drug_exposure_end_datetime,
        cast(prdc.service_line_to_date as date) as verbatim_end_date,
        32854 as drug_type_concept_id,
        cast(null as varchar) as stop_reason,
        0 as refills,
        cast(null as integer) as quantity,
        cast(prdc.days_units_billed as integer) as days_supply,
        cast(null as varchar) as sig,
        cast(null as integer) as route_concept_id,
        cast(null as integer) as lot_number,
        cast(
            hash(
                concat_ws(
                '||',
                prhc.rendering_bill_provider_last,
                coalesce(prhc.rendering_bill_provider_first, ''),
                coalesce(prhc.rendering_bill_provider_middle, ''),
                prhc.rendering_bill_provider_state_1,
                prhc.rendering_bill_provider_4
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as provider_id,
        cast(prdc.bill_id as varchar) as visit_occurrence_id,
        cast(null as integer) as visit_detail_id,
        prdc.hcpcs_line_procedure_billed as drug_source_value,
        cast(null as integer) as drug_source_concept_id,
        cast(null as varchar) as route_source_value,
        cast(null as varchar) as dose_unit_source_value
    from {{ source('raw','professional_detail_current') }} prdc
    join {{ source('raw','professional_header_current') }} prhc
        on cast(prdc.bill_id as varchar) = cast(prhc.bill_id as varchar)
    join {{ source('terminology','concept') }} as c
        on c.concept_code = prdc.hcpcs_line_procedure_billed
    where c.domain_id = 'Drug'
        and c.vocabulary_id = 'HCPCS'
)
{% endif %}

{% if exists_prd_historical %}
{% if exists_prd_current %}, {% endif %}
professional_detail_historical as (
    select 
        cast(
            hash(
                concat_ws(
                    '||',
                    prdh.bill_id,
                    prdh.row_id
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as drug_exposure_id,
        case 
            when prhh.patient_account_number is null or trim(prhh.patient_account_number) = '' then lpad(
                cast(
                    (
                        hash(
                            concat_ws(
                                '||',
                                coalesce(prhh.employee_mailing_city, ''),
                                coalesce(prhh.employee_mailing_state_code, ''),
                                coalesce(prhh.employee_mailing_postal_code, ''),
                                coalesce(prhh.employee_mailing_country, ''),
                                coalesce(cast(prhh.employee_date_of_birth as varchar), ''),
                                coalesce(prhh.employee_gender_code, '')
                            ),
                            'xxhash64'
                        ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
            )
            else prhh.patient_account_number
        end as person_id,
        cast(null as integer) as drug_concept_id,
        cast(prdh.service_line_from_date as date) as drug_exposure_start_date,
        cast(prdh.service_line_from_date as timestamp) as drug_exposure_start_datetime,
        cast(prdh.service_line_to_date as date) as drug_exposure_end_date,
        cast(prdh.service_line_to_date as timestamp) as drug_exposure_end_datetime,
        cast(prdh.service_line_to_date as date) as verbatim_end_date,
        32854 as drug_type_concept_id,
        cast(null as varchar) as stop_reason,
        0 as refills,
        cast(null as integer) as quantity,
        cast(prdh.days_units_billed as integer) as days_supply,
        cast(null as varchar) as sig,
        cast(null as integer) as route_concept_id,
        cast(null as integer) as lot_number,
        cast(
            hash(
                concat_ws(
                '||',
                prhh.rendering_bill_provider_last,
                coalesce(prhh.rendering_bill_provider_first, ''),
                coalesce(prhh.rendering_bill_provider_middle, ''),
                prhh.rendering_bill_provider_state_1,
                prhh.rendering_bill_provider_4
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as provider_id,
        cast(prdh.bill_id as varchar) as visit_occurrence_id,
        cast(null as integer) as visit_detail_id,
        prdh.hcpcs_line_procedure_billed as drug_source_value,
        cast(null as integer) as drug_source_concept_id,
        cast(null as varchar) as route_source_value,
        cast(null as varchar) as dose_unit_source_value
    from {{ source('raw','professional_detail_historical') }} prdh
    join {{ source('raw','professional_header_historical') }} prhh
        on cast(prdh.bill_id as varchar) = cast(prhh.bill_id as varchar)
    join {{ source('terminology','concept') }} as c
        on c.concept_code = prdh.hcpcs_line_procedure_billed
    where c.domain_id = 'Drug'
        and c.vocabulary_id = 'HCPCS'
)
{% endif %}

{% set cte_list = [] %}
{% if exists_phd_current %}
  {% set _ = cte_list.append("select * from pharmacy_detail_current") %}
{% endif %}
{% if exists_phd_historical %}
  {% set _ = cte_list.append("select * from pharmacy_detail_historical") %}
{% endif %}
{% if exists_id_current %}
  {% set _ = cte_list.append("select * from institutional_detail_current") %}
{% endif %}
{% if exists_id_historical %}
  {% set _ = cte_list.append("select * from institutional_detail_historical") %}
{% endif %}
{% if exists_prd_current %}
  {% set _ = cte_list.append("select * from professional_detail_current") %}
{% endif %}
{% if exists_prd_historical %}
  {% set _ = cte_list.append("select * from professional_detail_historical") %}
{% endif %}

select *
from (
    {{ cte_list | join(" union ") }}
) as final_result