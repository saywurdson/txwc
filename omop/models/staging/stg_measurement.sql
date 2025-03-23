{% set exists_i_current = check_table_exists('raw', 'institutional_header_current') %}
{% set exists_i_historical = check_table_exists('raw', 'institutional_header_historical') %}
{% set exists_pr_current = check_table_exists('raw', 'professional_header_current') %}
{% set exists_pr_historical = check_table_exists('raw', 'professional_header_historical') %}
{% set exists_ph_current = check_table_exists('raw', 'pharmacy_header_current') %}
{% set exists_ph_historical = check_table_exists('raw', 'pharmacy_header_historical') %}
{% set exists_id_current = check_table_exists('raw', 'institutional_detail_current') %}
{% set exists_id_historical = check_table_exists('raw', 'institutional_detail_historical') %}
{% set exists_prd_current = check_table_exists('raw', 'professional_detail_current') %}
{% set exists_prd_historical = check_table_exists('raw', 'professional_detail_historical') %}


with
{% if exists_i_current %}
-- Get columns needed
institutional_header_current as (
    select 
        bill_id,
        unique_bill_id_number,
        patient_account_number,
        service_bill_from_date,
        service_bill_to_date,
        employee_mailing_city,
        employee_mailing_state_code,
        employee_mailing_postal_code,
        employee_mailing_country,
        employee_date_of_birth,
        employee_gender_code,
        first_icd_9cm_or_icd_10cm,
        second_icd_9cm_or_icd_10cm,
        third_icd_9cm_or_icd_10cm,
        fourth_icd_9cm_or_icd_10cm,
        fifth_icd_9cm_or_icd_10cm,
        principal_diagnosis_code,
        admitting_diagnosis_code
    from {{ source('raw','institutional_header_current') }} as ihc
),
unpivot_ihc_diagnoses as (
    -- Unpivot the diagnoses and prioritize in order to assign the correct measurement_status_concept_id.
    select 
        ihc.bill_id,
        t.icd as measurement_source_value,
        t.source_column,
        t.priority,
        row_number() over (
            partition by ihc.bill_id, t.icd 
            order by t.priority
        ) as rn
    from institutional_header_current as ihc
    cross join lateral (
        values
            (first_icd_9cm_or_icd_10cm, 'first_icd_9cm_or_icd_10cm', 3),
            (second_icd_9cm_or_icd_10cm, 'second_icd_9cm_or_icd_10cm', 3),
            (third_icd_9cm_or_icd_10cm, 'third_icd_9cm_or_icd_10cm', 3),
            (fourth_icd_9cm_or_icd_10cm, 'fourth_icd_9cm_or_icd_10cm', 3),
            (fifth_icd_9cm_or_icd_10cm, 'fifth_icd_9cm_or_icd_10cm', 3),
            (principal_diagnosis_code, 'principal_diagnosis_code', 1),
            (admitting_diagnosis_code, 'admitting_diagnosis_code', 2)
    ) as t(icd, source_column, priority)
    join {{ source('omop','concept') }} as c
        on c.concept_code = t.icd
    where c.domain_id = 'Measurement'
        and c.vocabulary_id in ('ICD10CM','ICD9CM')
),
unique_ihc_diagnoses as (
    -- Keep only the highest-priority row per patient and diagnosis code.
    select bill_id, measurement_source_value, source_column, priority
    from unpivot_ihc_diagnoses
    where rn = 1
),
-- Final table creation
final_ihc as (
    select 
        cast(
            hash(
                concat_ws(
                '||',
                ihc.row_id,
                ihc.bill_id
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as measurement_id,
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
        cast(null as integer) as measurement_concept_id,
        cast(ihc.service_bill_from_date as date) as measurement_date,
        cast(ihc.service_bill_from_date as timestamp) as measurement_datetime,
        strftime(cast(ihc.service_bill_from_date AS timestamp), '%H:%M:%S') AS measurement_time,
        32855 as measurement_type_concept_id,
        cast(null as integer) as operator_concept_id,
        cast(null as float) as value_as_number,
        cast(null as integer) as value_as_concept_id,
        cast(null as integer) as unit_concept_id,
        cast(null as integer) as range_low,
        cast(null as integer) as range_high,
        cast(
            hash(
                concat_ws(
                '||',
                ihc.rendering_bill_provider_last,
                coalesce(ihc.rendering_bill_provider_first, ''),
                ihc.rendering_bill_provider_state_1,
                ihc.rendering_bill_provider_4
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as provider_id,
        cast(ihc.bill_id as varchar) as visit_occurrence_id,
        cast(null as integer) as visit_detail_id,
        uihcd.measurement_source_value,
        cast(null as integer) as measurement_source_concept_id,
        cast(null as varchar) as unit_source_value,
        cast(null as integer) as unit_source_concept_id,
        cast(null as varchar) as value_source_value,
        cast(null as integer) as measurement_event_id,
        cast(null as integer) as meas_event_field_concept_id
    from {{ source('raw','institutional_header_current') }} ihc
    join unique_ihc_diagnoses uihcd
      on cast(ihc.bill_id as varchar) = cast(uihcd.bill_id as varchar)
)
{% endif %}

{% if exists_i_historical %}
{% if exists_i_current %}, {% endif %}
-- Get columns needed
institutional_header_historical as (
    select 
        bill_id,
        unique_bill_id_number,
        patient_account_number,
        service_bill_from_date,
        service_bill_to_date,
        employee_mailing_city,
        employee_mailing_state_code,
        employee_mailing_postal_code,
        employee_mailing_country,
        employee_date_of_birth,
        employee_gender_code,
        first_icd_9cm_or_icd_10cm,
        second_icd_9cm_or_icd_10cm,
        third_icd_9cm_or_icd_10cm,
        fourth_icd_9cm_or_icd_10cm,
        fifth_icd_9cm_or_icd_10cm,
        principal_diagnosis_code,
        admitting_diagnosis_code
    from {{ source('raw','institutional_header_historical') }} as ihh
),
unpivot_ihh_diagnoses as (
    -- Unpivot the diagnoses and prioritize in order to assign the correct measurement_status_concept_id.
    select 
        ihh.bill_id,
        t.icd as measurement_source_value,
        t.source_column,
        t.priority,
        row_number() over (
            partition by ihh.bill_id, t.icd 
            order by t.priority
        ) as rn
    from institutional_header_historical as ihh
    cross join lateral (
        values
            (first_icd_9cm_or_icd_10cm, 'first_icd_9cm_or_icd_10cm', 3),
            (second_icd_9cm_or_icd_10cm, 'second_icd_9cm_or_icd_10cm', 3),
            (third_icd_9cm_or_icd_10cm, 'third_icd_9cm_or_icd_10cm', 3),
            (fourth_icd_9cm_or_icd_10cm, 'fourth_icd_9cm_or_icd_10cm', 3),
            (fifth_icd_9cm_or_icd_10cm, 'fifth_icd_9cm_or_icd_10cm', 3),
            (principal_diagnosis_code, 'principal_diagnosis_code', 1),
            (admitting_diagnosis_code, 'admitting_diagnosis_code', 2)
    ) as t(icd, source_column, priority)
    join {{ source('omop','concept') }} as c
        on c.concept_code = t.icd
    where c.domain_id = 'Measurement'
        and c.vocabulary_id in ('ICD10CM','ICD9CM')
),
unique_ihh_diagnoses as (
    -- Keep only the highest-priority row per patient and diagnosis code.
    select bill_id, measurement_source_value, source_column, priority
    from unpivot_ihh_diagnoses
    where rn = 1
),
-- Final table creation
final_ihh as (
    select 
        cast(
            hash(
                concat_ws(
                '||',
                ihh.row_id,
                ihh.bill_id
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as measurement_id,
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
        cast(null as integer) as measurement_concept_id,
        cast(ihh.service_bill_from_date as date) as measurement_date,
        cast(ihh.service_bill_from_date as timestamp) as measurement_datetime,
        strftime(cast(ihh.service_bill_from_date AS timestamp), '%H:%M:%S') AS measurement_time,
        32855 as measurement_type_concept_id,
        cast(null as integer) as operator_concept_id,
        cast(null as float) as value_as_number,
        cast(null as integer) as value_as_concept_id,
        cast(null as integer) as unit_concept_id,
        cast(null as integer) as range_low,
        cast(null as integer) as range_high,
        cast(
            hash(
                concat_ws(
                '||',
                ihh.rendering_bill_provider_last,
                coalesce(ihh.rendering_bill_provider_first, ''),
                ihh.rendering_bill_provider_state_1,
                ihh.rendering_bill_provider_4
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as provider_id,
        cast(ihh.bill_id as varchar) as visit_occurrence_id,
        cast(null as integer) as visit_detail_id,
        uihhd.measurement_source_value,
        cast(null as integer) as measurement_source_concept_id,
        cast(null as varchar) as unit_source_value,
        cast(null as integer) as unit_source_concept_id,
        cast(null as varchar) as value_source_value,
        cast(null as integer) as measurement_event_id,
        cast(null as integer) as meas_event_field_concept_id
    from {{ source('raw','institutional_header_historical') }} ihh
    join unique_ihh_diagnoses uihhd
      on cast(ihh.bill_id as varchar) = cast(uihhd.bill_id as varchar)
)
{% endif %}

{% if exists_pr_historical %}
{% if exists_i_historical %}, {% endif %}
-- Get columns needed
professional_header_historical as (
    select 
        bill_id,
        unique_bill_id_number,
        patient_account_number,
        service_bill_from_date,
        service_bill_to_date,
        employee_mailing_city,
        employee_mailing_state_code,
        employee_mailing_postal_code,
        employee_mailing_country,
        employee_date_of_birth,
        employee_gender_code,
        first_icd_9cm_or_icd_10cm,
        second_icd_9cm_or_icd_10cm,
        third_icd_9cm_or_icd_10cm,
        fourth_icd_9cm_or_icd_10cm,
        fifth_icd_9cm_or_icd_10cm
    from {{ source('raw','professional_header_historical') }} as phh
),
unpivot_phh_diagnoses as (
    select 
        phh.bill_id,
        t.icd as measurement_source_value,
        t.source_column
    from professional_header_historical as phh
    cross join lateral (
        values
            (first_icd_9cm_or_icd_10cm, 'first_icd_9cm_or_icd_10cm'),
            (second_icd_9cm_or_icd_10cm, 'second_icd_9cm_or_icd_10cm'),
            (third_icd_9cm_or_icd_10cm, 'third_icd_9cm_or_icd_10cm'),
            (fourth_icd_9cm_or_icd_10cm, 'fourth_icd_9cm_or_icd_10cm'),
            (fifth_icd_9cm_or_icd_10cm, 'fifth_icd_9cm_or_icd_10cm')
    ) as t(icd, source_column)
    join {{ source('omop','concept') }} as c
        on c.concept_code = t.icd
    where c.domain_id = 'Measurement'
        and c.vocabulary_id in ('ICD10CM','ICD9CM')
),
-- Final table creation
final_phh as (
    select 
        cast(
            hash(
                concat_ws(
                '||',
                phh.row_id,
                phh.bill_id
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as measurement_id,
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
        cast(null as integer) as measurement_concept_id,
        cast(phh.service_bill_from_date as date) as measurement_date,
        cast(phh.service_bill_from_date as timestamp) as measurement_datetime,
        strftime(cast(phh.service_bill_from_date AS timestamp), '%H:%M:%S') AS measurement_time,
        32855 as measurement_type_concept_id,
        cast(null as integer) as operator_concept_id,
        cast(null as float) as value_as_number,
        cast(null as integer) as value_as_concept_id,
        cast(null as integer) as unit_concept_id,
        cast(null as integer) as range_low,
        cast(null as integer) as range_high,
        cast(
            hash(
                concat_ws(
                '||',
                phh.rendering_bill_provider_last,
                coalesce(phh.rendering_bill_provider_first, ''),
                phh.rendering_bill_provider_state_1,
                phh.rendering_bill_provider_4
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as provider_id,
        cast(phh.bill_id as varchar) as visit_occurrence_id,
        cast(null as integer) as visit_detail_id,
        uphhd.measurement_source_value,
        cast(null as integer) as measurement_source_concept_id,
        cast(null as varchar) as unit_source_value,
        cast(null as integer) as unit_source_concept_id,
        cast(null as varchar) as value_source_value,
        cast(null as integer) as measurement_event_id,
        cast(null as integer) as meas_event_field_concept_id
    from {{ source('raw','professional_header_historical') }} phh
    join unpivot_phh_diagnoses uphhd
      on cast(phh.bill_id as varchar) = cast(uphhd.bill_id as varchar)
)
{% endif %}

{% if exists_pr_current %}
{% if exists_pr_historical %}, {% endif %}
-- Get columns needed
professional_header_current as (
    select 
        bill_id,
        unique_bill_id_number,
        patient_account_number,
        service_bill_from_date,
        service_bill_to_date,
        employee_mailing_city,
        employee_mailing_state_code,
        employee_mailing_postal_code,
        employee_mailing_country,
        employee_date_of_birth,
        employee_gender_code,
        first_icd_9cm_or_icd_10cm,
        second_icd_9cm_or_icd_10cm,
        third_icd_9cm_or_icd_10cm,
        fourth_icd_9cm_or_icd_10cm,
        fifth_icd_9cm_or_icd_10cm
    from {{ source('raw','professional_header_current') }} as phc
),
unpivot_phc_diagnoses as (
    select 
        phc.bill_id,
        t.icd as measurement_source_value,
        t.source_column
    from professional_header_current as phc
    cross join lateral (
        values
            (first_icd_9cm_or_icd_10cm, 'first_icd_9cm_or_icd_10cm'),
            (second_icd_9cm_or_icd_10cm, 'second_icd_9cm_or_icd_10cm'),
            (third_icd_9cm_or_icd_10cm, 'third_icd_9cm_or_icd_10cm'),
            (fourth_icd_9cm_or_icd_10cm, 'fourth_icd_9cm_or_icd_10cm'),
            (fifth_icd_9cm_or_icd_10cm, 'fifth_icd_9cm_or_icd_10cm')
    ) as t(icd, source_column)
    join {{ source('omop','concept') }} as c
        on c.concept_code = t.icd
    where c.domain_id = 'Measurement'
        and c.vocabulary_id in ('ICD10CM','ICD9CM')
),
-- Final table creation
final_phc as (
    select 
        cast(
            hash(
                concat_ws(
                '||',
                phc.row_id,
                phc.bill_id
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as measurement_id,
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
        cast(null as integer) as measurement_concept_id,
        cast(phc.service_bill_from_date as date) as measurement_date,
        cast(phc.service_bill_from_date as timestamp) as measurement_datetime,
        strftime(cast(phc.service_bill_from_date AS timestamp), '%H:%M:%S') AS measurement_time,
        32855 as measurement_type_concept_id,
        cast(null as integer) as operator_concept_id,
        cast(null as float) as value_as_number,
        cast(null as integer) as value_as_concept_id,
        cast(null as integer) as unit_concept_id,
        cast(null as integer) as range_low,
        cast(null as integer) as range_high,
        cast(
            hash(
                concat_ws(
                '||',
                phc.rendering_bill_provider_last,
                coalesce(phc.rendering_bill_provider_first, ''),
                phc.rendering_bill_provider_state_1,
                phc.rendering_bill_provider_4
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as provider_id,
        cast(phc.bill_id as varchar) as visit_occurrence_id,
        cast(null as integer) as visit_detail_id,
        uphcd.measurement_source_value,
        cast(null as integer) as measurement_source_concept_id,
        cast(null as varchar) as unit_source_value,
        cast(null as integer) as unit_source_concept_id,
        cast(null as varchar) as value_source_value,
        cast(null as integer) as measurement_event_id,
        cast(null as integer) as meas_event_field_concept_id
    from {{ source('raw','professional_header_current') }} phc
    join unpivot_phc_diagnoses uphcd
      on cast(phc.bill_id as varchar) = cast(uphcd.bill_id as varchar)
)
{% endif %}

{% if exists_id_current %}
{% if exists_pr_current %}, {% endif %}
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
        as varchar) as measurement_id,
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
        cast(null as integer) as measurement_concept_id,
        cast(idc.service_line_from_date as date) as measurement_date,
        cast(idc.service_line_from_date as timestamp) as measurement_datetime,
        strftime(cast(idc.service_line_from_date AS timestamp), '%H:%M:%S') AS measurement_time,
        32854 as measurement_type_concept_id,
        cast(null as integer) as operator_concept_id,
        cast(null as float) as value_as_number,
        cast(null as integer) as value_as_concept_id,
        cast(null as integer) as unit_concept_id,
        cast(null as integer) as range_low,
        cast(null as integer) as range_high,
        cast(
            hash(
                concat_ws(
                '||',
                ihc.rendering_bill_provider_last,
                coalesce(ihc.rendering_bill_provider_first, ''),
                ihc.rendering_bill_provider_state_1,
                ihc.rendering_bill_provider_4
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as provider_id,
        cast(idc.bill_id as varchar) as visit_occurrence_id,
        cast(null as integer) as visit_detail_id,
        idc.hcpcs_line_procedure_billed as measurement_source_value,
        cast(null as integer) as measurement_source_concept_id,
        cast(null as varchar) as unit_source_value,
        cast(null as integer) as unit_source_concept_id,
        cast(null as varchar) as value_source_value,
        cast(null as integer) as measurement_event_id,
        cast(null as integer) as meas_event_field_concept_id
    from {{ source('raw','institutional_detail_current') }} idc
    join {{ source('raw','institutional_header_current') }} ihc
        on cast(idc.bill_id as varchar) = cast(ihc.bill_id as varchar)
    join {{ source('omop','concept') }} as c
        on c.concept_code = idc.hcpcs_line_procedure_billed
    where c.domain_id = 'Measurement'
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
        as varchar) as measurement_id,
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
        cast(null as integer) as measurement_concept_id,
        cast(idh.service_line_from_date as date) as measurement_date,
        cast(idh.service_line_from_date as timestamp) as measurement_datetime,
        strftime(cast(idh.service_line_from_date AS timestamp), '%H:%M:%S') AS measurement_time,
        32854 as measurement_type_concept_id,
        cast(null as integer) as operator_concept_id,
        cast(null as float) as value_as_number,
        cast(null as integer) as value_as_concept_id,
        cast(null as integer) as unit_concept_id,
        cast(null as integer) as range_low,
        cast(null as integer) as range_high,
        cast(
            hash(
                concat_ws(
                '||',
                ihh.rendering_bill_provider_last,
                coalesce(ihh.rendering_bill_provider_first, ''),
                ihh.rendering_bill_provider_state_1,
                ihh.rendering_bill_provider_4
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as provider_id,
        cast(idh.bill_id as varchar) as visit_occurrence_id,
        cast(null as integer) as visit_detail_id,
        idh.hcpcs_line_procedure_billed as measurement_source_value,
        cast(null as integer) as measurement_source_concept_id,
        cast(null as varchar) as unit_source_value,
        cast(null as integer) as unit_source_concept_id,
        cast(null as varchar) as value_source_value,
        cast(null as integer) as measurement_event_id,
        cast(null as integer) as meas_event_field_concept_id
    from {{ source('raw','institutional_detail_historical') }} idh
    join {{ source('raw','institutional_header_historical') }} ihh
        on cast(idh.bill_id as varchar) = cast(ihh.bill_id as varchar)
    join {{ source('omop','concept') }} as c
        on c.concept_code = idh.hcpcs_line_procedure_billed
    where c.domain_id = 'Measurement'
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
        as varchar) as measurement_id,
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
        cast(null as integer) as measurement_concept_id,
        cast(prdc.service_line_from_date as date) as measurement_date,
        cast(prdc.service_line_from_date as timestamp) as measurement_datetime,
        strftime(cast(prdc.service_line_from_date AS timestamp), '%H:%M:%S') AS measurement_time,
        32854 as measurement_type_concept_id,
        cast(null as integer) as operator_concept_id,
        cast(null as float) as value_as_number,
        cast(null as integer) as value_as_concept_id,
        cast(null as integer) as unit_concept_id,
        cast(null as integer) as range_low,
        cast(null as integer) as range_high,
        cast(
            hash(
                concat_ws(
                '||',
                prhc.rendering_bill_provider_last,
                coalesce(prhc.rendering_bill_provider_first, ''),
                prhc.rendering_bill_provider_state_1,
                prhc.rendering_bill_provider_4
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as provider_id,
        cast(prdc.bill_id as varchar) as visit_occurrence_id,
        cast(null as integer) as visit_detail_id,
        prdc.hcpcs_line_procedure_billed as measurement_source_value,
        cast(null as integer) as measurement_source_concept_id,
        cast(null as varchar) as unit_source_value,
        cast(null as integer) as unit_source_concept_id,
        cast(null as varchar) as value_source_value,
        cast(null as integer) as measurement_event_id,
        cast(null as integer) as meas_event_field_concept_id
    from {{ source('raw','professional_detail_current') }} prdc
    join {{ source('raw','professional_header_current') }} prhc
        on cast(prdc.bill_id as varchar) = cast(prhc.bill_id as varchar)
    join {{ source('omop','concept') }} as c
        on c.concept_code = prdc.hcpcs_line_procedure_billed
    where c.domain_id = 'Measurement'
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
        as varchar) as measurement_id,
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
        cast(null as integer) as measurement_concept_id,
        cast(prdh.service_line_from_date as date) as measurement_date,
        cast(prdh.service_line_from_date as timestamp) as measurement_datetime,
        strftime(cast(prdh.service_line_from_date AS timestamp), '%H:%M:%S') AS measurement_time,
        32854 as measurement_type_concept_id,
        cast(null as integer) as operator_concept_id,
        cast(null as float) as value_as_number,
        cast(null as integer) as value_as_concept_id,
        cast(null as integer) as unit_concept_id,
        cast(null as integer) as range_low,
        cast(null as integer) as range_high,
        cast(
            hash(
                concat_ws(
                '||',
                prhh.rendering_bill_provider_last,
                coalesce(prhh.rendering_bill_provider_first, ''),
                prhh.rendering_bill_provider_state_1,
                prhh.rendering_bill_provider_4
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as provider_id,
        cast(prdh.bill_id as varchar) as visit_occurrence_id,
        cast(null as integer) as visit_detail_id,
        prdh.hcpcs_line_procedure_billed as measurement_source_value,
        cast(null as integer) as measurement_source_concept_id,
        cast(null as varchar) as unit_source_value,
        cast(null as integer) as unit_source_concept_id,
        cast(null as varchar) as value_source_value,
        cast(null as integer) as measurement_event_id,
        cast(null as integer) as meas_event_field_concept_id
    from {{ source('raw','professional_detail_historical') }} prdh
    join {{ source('raw','professional_header_historical') }} prhh
        on cast(prdh.bill_id as varchar) = cast(prhh.bill_id as varchar)
    join {{ source('omop','concept') }} as c
        on c.concept_code = prdh.hcpcs_line_procedure_billed
    where c.domain_id = 'Measurement'
        and c.vocabulary_id = 'HCPCS'
)
{% endif %}

{% set cte_list = [] %}
{% if exists_i_current %}
  {% set _ = cte_list.append("select * from final_ihc") %}
{% endif %}
{% if exists_i_historical %}
  {% set _ = cte_list.append("select * from final_ihh") %}
{% endif %}
{% if exists_pr_current %}
  {% set _ = cte_list.append("select * from final_phc") %}
{% endif %}
{% if exists_pr_historical %}
  {% set _ = cte_list.append("select * from final_phh") %}
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