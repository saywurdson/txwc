{% set exists_i_current = check_table_exists('raw', 'institutional_header_current') %}
{% set exists_i_historical = check_table_exists('raw', 'institutional_header_historical') %}
{% set exists_id_current = check_table_exists('raw', 'institutional_detail_current') %}
{% set exists_id_historical = check_table_exists('raw', 'institutional_detail_historical') %}
{% set exists_prd_current = check_table_exists('raw', 'professional_detail_current') %}
{% set exists_prd_historical = check_table_exists('raw', 'professional_detail_historical') %}

with
{% if exists_i_current %}
-- get columns needed
institutional_header_current as (
    select 
        bill_id,
        unique_bill_id_number,
        patient_account_number,
        principal_procedure_date,
        first_procedure_date,
        second_procedure_date,
        third_procedure_date,
        fourth_procedure_date,
        employee_mailing_city,
        employee_mailing_state_code,
        employee_mailing_postal_code,
        employee_mailing_country,
        employee_date_of_birth,
        employee_gender_code,
        first_icd_9cm_or_icd_10cm_1,
        second_icd_9cm_or_icd_10cm_1,
        third_icd_9cm_or_icd_10cm_1,
        fourth_icd_9cm_or_icd_10cm_1,
        icd_9cm_or_icd_10cm_principal
    from {{ source('raw','institutional_header_current') }} as ihc
),
unpivot_ihc_diagnoses as (
    -- unpivot the diagnoses and prioritize in order to assign the correct procedure code
    select 
        ihc.bill_id,
        t.icd as procedure_source_value,
        t.source_column
    from institutional_header_current as ihc
    cross join lateral (
        values
            (first_icd_9cm_or_icd_10cm_1, 'first_icd_9cm_or_icd_10cm'),
            (second_icd_9cm_or_icd_10cm_1, 'second_icd_9cm_or_icd_10cm'),
            (third_icd_9cm_or_icd_10cm_1, 'third_icd_9cm_or_icd_10cm'),
            (fourth_icd_9cm_or_icd_10cm_1, 'fourth_icd_9cm_or_icd_10cm'),
            (icd_9cm_or_icd_10cm_principal, 'icd_9cm_or_icd_10cm_principal')
    ) as t(icd, source_column)
    join {{ source('omop','concept') }} as c
        on c.concept_code = t.icd
    where c.domain_id = 'Procedure'
        and c.vocabulary_id in ('ICD10PCS','ICD9Proc')
),
final_ihc as (
    select 
        cast(
            hash(
                concat_ws(
                    '||',
                    ihc.bill_id,
                    ihc.row_id
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as procedure_occurrence_id,
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
        cast(null as integer) as procedure_concept_id,
        case 
            when uihcd.source_column = 'icd_9cm_or_icd_10cm_principal_1' then cast(ihc.principal_procedure_date as date)
            when uihcd.source_column = 'first_icd_9cm_or_icd_10cm_1' then cast(ihc.first_procedure_date as date)
            when uihcd.source_column = 'second_icd_9cm_or_icd_10cm_1' then cast(ihc.second_procedure_date as date)
            when uihcd.source_column = 'third_icd_9cm_or_icd_10cm_1' then cast(ihc.third_procedure_date as date)
            when uihcd.source_column = 'fourth_icd_9cm_or_icd_10cm_1' then cast(ihc.fourth_procedure_date as date)
            else cast(ihc.principal_procedure_date as date)
        end as procedure_date,
        case 
            when uihcd.source_column = 'icd_9cm_or_icd_10cm_principal_1' then cast(ihc.principal_procedure_date as timestamp)
            when uihcd.source_column = 'first_icd_9cm_or_icd_10cm_1' then cast(ihc.first_procedure_date as timestamp)
            when uihcd.source_column = 'second_icd_9cm_or_icd_10cm_1' then cast(ihc.second_procedure_date as timestamp)
            when uihcd.source_column = 'third_icd_9cm_or_icd_10cm_1' then cast(ihc.third_procedure_date as timestamp)
            when uihcd.source_column = 'fourth_icd_9cm_or_icd_10cm_1' then cast(ihc.fourth_procedure_date as timestamp)
            else cast(ihc.principal_procedure_date as timestamp)
        end as procedure_datetime,
        cast(null as date) as procedure_end_date,
        cast(null as timestamp) as procedure_end_datetime,
        32855 as procedure_type_concept_id,
        cast(null as integer) modifier_concept_id,
        1 as quantity,
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
        uihcd.procedure_source_value,
        cast(null as integer) as procedure_source_concept_id,
        cast(null as varchar) as modifier_source_value
    from {{ source('raw','institutional_header_current') }} ihc 
    join unpivot_ihc_diagnoses uihcd
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
        principal_procedure_date,
        first_procedure_date,
        second_procedure_date,
        third_procedure_date,
        fourth_procedure_date,
        employee_mailing_city,
        employee_mailing_state_code,
        employee_mailing_postal_code,
        employee_mailing_country,
        employee_date_of_birth,
        employee_gender_code,
        first_icd_9cm_or_icd_10cm_1,
        second_icd_9cm_or_icd_10cm_1,
        third_icd_9cm_or_icd_10cm_1,
        fourth_icd_9cm_or_icd_10cm_1,
        icd_9cm_or_icd_10cm_principal
    from {{ source('raw','institutional_header_historical') }} as ihh
),
unpivot_ihh_diagnoses as (
    select 
        ihh.bill_id,
        t.icd as procedure_source_value,
        t.source_column
    from institutional_header_historical as ihh
    cross join lateral (
        values
            (first_icd_9cm_or_icd_10cm_1, 'first_icd_9cm_or_icd_10cm'),
            (second_icd_9cm_or_icd_10cm_1, 'second_icd_9cm_or_icd_10cm'),
            (third_icd_9cm_or_icd_10cm_1, 'third_icd_9cm_or_icd_10cm'),
            (fourth_icd_9cm_or_icd_10cm_1, 'fourth_icd_9cm_or_icd_10cm'),
            (icd_9cm_or_icd_10cm_principal, 'icd_9cm_or_icd_10cm_principal')
    ) as t(icd, source_column)
    join {{ source('omop','concept') }} as c
        on c.concept_code = t.icd
    where c.domain_id = 'Procedure'
        and c.vocabulary_id in ('ICD10PCS','ICD9Proc')
),
final_ihh as (
    select 
        cast(
            hash(
                concat_ws(
                    '||',
                    ihh.bill_id,
                    ihh.row_id
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as procedure_occurrence_id,
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
        cast(null as integer) as procedure_concept_id,
        case 
            when uihhd.source_column = 'icd_9cm_or_icd_10cm_principal_1' then cast(ihh.principal_procedure_date as date)
            when uihhd.source_column = 'first_icd_9cm_or_icd_10cm_1' then cast(ihh.first_procedure_date as date)
            when uihhd.source_column = 'second_icd_9cm_or_icd_10cm_1' then cast(ihh.second_procedure_date as date)
            when uihhd.source_column = 'third_icd_9cm_or_icd_10cm_1' then cast(ihh.third_procedure_date as date)
            when uihhd.source_column = 'fourth_icd_9cm_or_icd_10cm_1' then cast(ihh.fourth_procedure_date as date)
            else cast(ihh.principal_procedure_date as date)
        end as procedure_date,
        case 
            when uihhd.source_column = 'icd_9cm_or_icd_10cm_principal_1' then cast(ihh.principal_procedure_date as timestamp)
            when uihhd.source_column = 'first_icd_9cm_or_icd_10cm_1' then cast(ihh.first_procedure_date as timestamp)
            when uihhd.source_column = 'second_icd_9cm_or_icd_10cm_1' then cast(ihh.second_procedure_date as timestamp)
            when uihhd.source_column = 'third_icd_9cm_or_icd_10cm_1' then cast(ihh.third_procedure_date as timestamp)
            when uihhd.source_column = 'fourth_icd_9cm_or_icd_10cm_1' then cast(ihh.fourth_procedure_date as timestamp)
            else cast(ihh.principal_procedure_date as timestamp)
        end as procedure_datetime,
        cast(null as date) as procedure_end_date,
        cast(null as timestamp) as procedure_end_datetime,
        32855 as procedure_type_concept_id,
        cast(null as integer) modifier_concept_id,
        1 as quantity,
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
        uihhd.procedure_source_value,
        cast(null as integer) as procedure_source_concept_id,
        cast(null as varchar) as modifier_source_value
    from {{ source('raw','institutional_header_historical') }} ihh
    join unpivot_ihh_diagnoses uihhd
        on cast(ihh.bill_id as varchar) = cast(uihhd.bill_id as varchar)
)
{% endif %}

{% if exists_id_current %}
{% if exists_i_historical %}, {% endif %}
institutional_detail_current as (
    select 
        bill_id,
        bill_detail_id,
        service_line_from_date,
        service_line_to_date,
        days_units_billed,
        hcpcs_line_procedure_billed,
        first_hcpcs_modifier_billed
    from {{ source('raw','institutional_detail_current') }}
),
final_id as (
    select
        cast(
            hash(
                concat_ws(
                    '||',
                    id.bill_id,
                    id.row_id
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as procedure_occurrence_id,
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
        cast(null as integer) as procedure_concept_id,
        cast(id.service_line_from_date as date) as procedure_date,
        cast(id.service_line_from_date as timestamp) as procedure_datetime,
        cast(id.service_line_to_date as date) as procedure_end_date,
        cast(id.service_line_to_date as timestamp) as procedure_end_datetime,
        32854 as procedure_type_concept_id,
        cast(null as integer) as modifier_concept_id,
        cast(id.days_units_billed as integer) as quantity,
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
        cast(id.bill_id as varchar) as visit_occurrence_id,
        cast(null as integer) as visit_detail_id,
        id.hcpcs_line_procedure_billed as procedure_source_value,
        cast(null as integer) as procedure_source_concept_id,
        id.first_hcpcs_modifier_billed as modifier_source_value
    from {{ source('raw','institutional_detail_current') }} id
    join {{ source('raw','institutional_header_current') }} ihc
        on id.bill_id = ihc.bill_id
    join {{ source('omop','concept') }} as c
        on c.concept_code = id.hcpcs_line_procedure_billed
    where c.domain_id = 'Procedure'
        and c.vocabulary_id in ('CPT4','HCPCS')
)
{% endif %}

{% if exists_id_historical %}
{% if exists_id_current %}, {% endif %}
institutional_detail_historical as (
    select 
        bill_id,
        bill_detail_id,
        service_line_from_date,
        service_line_to_date,
        days_units_billed,
        hcpcs_line_procedure_billed,
        first_hcpcs_modifier_billed
    from {{ source('raw','institutional_detail_historical') }}
),
final_idh as (
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
        as varchar) as procedure_occurrence_id,
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
        cast(null as integer) as procedure_concept_id,
        cast(idh.service_line_from_date as date) as procedure_date,
        cast(idh.service_line_from_date as timestamp) as procedure_datetime,
        cast(idh.service_line_to_date as date) as procedure_end_date,
        cast(idh.service_line_to_date as timestamp) as procedure_end_datetime,
        32854 as procedure_type_concept_id,
        cast(null as integer) as modifier_concept_id,
        cast(idh.days_units_billed as integer) as quantity,
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
        idh.hcpcs_line_procedure_billed as procedure_source_value,
        cast(null as integer) as procedure_source_concept_id,
        idh.first_hcpcs_modifier_billed as modifier_source_value
    from {{ source('raw','institutional_detail_historical') }} idh
    join {{ source('raw','institutional_header_historical') }} ihh
        on idh.bill_id = ihh.bill_id
    join {{ source('omop','concept') }} as c
        on c.concept_code = idh.hcpcs_line_procedure_billed
    where c.domain_id = 'Procedure'
        and c.vocabulary_id in ('CPT4','HCPCS')
)
{% endif %}

{% if exists_prd_current %}
{% if exists_id_historical %}, {% endif %}
professional_detail_current as (
    select 
        bill_id,
        bill_detail_id,
        service_line_from_date,
        service_line_to_date,
        days_units_billed,
        hcpcs_line_procedure_billed,
        first_hcpcs_modifier_billed
    from {{ source('raw','professional_detail_current') }}
),
final_pdc as (
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
        as varchar) as procedure_occurrence_id,
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
        cast(null as integer) as procedure_concept_id,
        cast(pdc.service_line_from_date as date) as procedure_date,
        cast(pdc.service_line_from_date as timestamp) as procedure_datetime,
        cast(pdc.service_line_to_date as date) as procedure_end_date,
        cast(pdc.service_line_to_date as timestamp) as procedure_end_datetime,
        32872 as procedure_type_concept_id,
        cast(null as integer) as modifier_concept_id,
        cast(pdc.days_units_billed as integer) as quantity,
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
        cast(pdc.bill_id as varchar) as visit_occurrence_id,
        cast(null as integer) as visit_detail_id,
        pdc.hcpcs_line_procedure_billed as procedure_source_value,
        cast(null as integer) as procedure_source_concept_id,
        pdc.first_hcpcs_modifier_billed as modifier_source_value
    from {{ source('raw','professional_detail_current') }} pdc
    join {{ source('raw','professional_header_current') }} prhc
        on pdc.bill_id = prhc.bill_id
    join {{ source('omop','concept') }} as c
        on c.concept_code = pdc.hcpcs_line_procedure_billed
    where c.domain_id = 'Procedure'
        and c.vocabulary_id in ('CPT4','HCPCS')
)
{% endif %}

{% if exists_prd_historical %}
{% if exists_prd_current %}, {% endif %}
professional_detail_historical as (
    select 
        bill_id,
        bill_detail_id,
        service_line_from_date,
        service_line_to_date,
        days_units_billed,
        hcpcs_line_procedure_billed,
        first_hcpcs_modifier_billed
    from {{ source('raw','professional_detail_historical') }}
),
final_pdh as (
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
        as varchar) as procedure_occurrence_id,
        case 
            when phhc.patient_account_number is null or trim(phhc.patient_account_number) = '' then lpad(
                cast(
                    (
                        hash(
                            concat_ws(
                                '||',
                                coalesce(phhc.employee_mailing_city, ''),
                                coalesce(phhc.employee_mailing_state_code, ''),
                                coalesce(phhc.employee_mailing_postal_code, ''),
                                coalesce(phhc.employee_mailing_country, ''),
                                coalesce(cast(phhc.employee_date_of_birth as varchar), ''),
                                coalesce(phhc.employee_gender_code, '')
                            ),
                            'xxhash64'
                        ) % 1000000000
                    ) as varchar
                ),
                9,
                '0'
            )
            else phhc.patient_account_number
        end as person_id,
        cast(null as integer) as procedure_concept_id,
        cast(pdh.service_line_from_date as date) as procedure_date,
        cast(pdh.service_line_from_date as timestamp) as procedure_datetime,
        cast(pdh.service_line_to_date as date) as procedure_end_date,
        cast(pdh.service_line_to_date as timestamp) as procedure_end_datetime,
        32872 as procedure_type_concept_id,
        cast(null as integer) as modifier_concept_id,
        cast(pdh.days_units_billed as integer) as quantity,
        cast(
            hash(
                concat_ws(
                '||',
                phhc.rendering_bill_provider_last,
                coalesce(phhc.rendering_bill_provider_first, ''),
                phhc.rendering_bill_provider_state_1,
                phhc.rendering_bill_provider_4
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as provider_id,
        cast(pdh.bill_id as varchar) as visit_occurrence_id,
        cast(null as integer) as visit_detail_id,
        pdh.hcpcs_line_procedure_billed as procedure_source_value,
        cast(null as integer) as procedure_source_concept_id,
        pdh.first_hcpcs_modifier_billed as modifier_source_value
    from {{ source('raw','professional_detail_historical') }} pdh
    join {{ source('raw','professional_header_historical') }} phhc
        on pdh.bill_id = phhc.bill_id
    join {{ source('omop','concept') }} as c
        on c.concept_code = pdh.hcpcs_line_procedure_billed
    where c.domain_id = 'Procedure'
        and c.vocabulary_id in ('CPT4','HCPCS')
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
  {% set _ = cte_list.append("select * from final_id") %}
{% endif %}
{% if exists_id_historical %}
  {% set _ = cte_list.append("select * from final_idh") %}
{% endif %}
{% if exists_prd_current %}
  {% set _ = cte_list.append("select * from final_pdc") %}
{% endif %}
{% if exists_prd_historical %}
  {% set _ = cte_list.append("select * from final_pdh") %}
{% endif %}

select *
from (
    {{ cte_list | join(" union ") }}
) as final_result