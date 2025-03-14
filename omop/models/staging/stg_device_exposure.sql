{% set exists_id_current = check_table_exists('raw', 'institutional_detail_current') %}
{% set exists_id_historical = check_table_exists('raw', 'institutional_detail_historical') %}
{% set exists_prd_current = check_table_exists('raw', 'professional_detail_current') %}
{% set exists_prd_historical = check_table_exists('raw', 'professional_detail_historical') %}

with
{% if exists_id_current %}
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
        as varchar) as device_exposure_id,
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
        cast(null as integer) as device_concept_id,
        cast(idc.service_line_from_date as date) as device_exposure_start_date,
        cast(idc.service_line_from_date as timestamp) as device_exposure_start_datetime,
        cast(idc.service_line_to_date as date) as device_exposure_end_date,
        cast(idc.service_line_to_date as timestamp) as device_exposure_end_datetime,
        32854 as device_type_concept_id,
        cast(null as varchar) as unique_device_id,
        cast(null as varchar) as production_id,
        1 as quantity,
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
        idc.hcpcs_line_procedure_billed as device_source_value,
        cast(null as integer) as device_source_concept_id,
        cast(null as integer) as unit_concept_id,
        cast(null as varchar) as unit_source_value,
        cast(null as varchar) as unit_source_concept_id
    from {{ source('raw','institutional_detail_current') }} idc
    join {{ source('raw','institutional_header_current') }} ihc
        on cast(idc.bill_id as varchar) = cast(ihc.bill_id as varchar)
    join {{ source('omop','concept') }} as c
        on c.concept_code = idc.hcpcs_line_procedure_billed
    where c.domain_id = 'Device'
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
        as varchar) as device_exposure_id,
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
        cast(null as integer) as device_concept_id,
        cast(idh.service_line_from_date as date) as device_exposure_start_date,
        cast(idh.service_line_from_date as timestamp) as device_exposure_start_datetime,
        cast(idh.service_line_to_date as date) as device_exposure_end_date,
        cast(idh.service_line_to_date as timestamp) as device_exposure_end_datetime,
        32854 as device_type_concept_id,
        cast(null as varchar) as unique_device_id,
        cast(null as varchar) as production_id,
        1 as quantity,
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
        idh.hcpcs_line_procedure_billed as device_source_value,
        cast(null as integer) as device_source_concept_id,
        cast(null as integer) as unit_concept_id,
        cast(null as varchar) as unit_source_value,
        cast(null as varchar) as unit_source_concept_id
    from {{ source('raw','institutional_detail_historical') }} idh
    join {{ source('raw','institutional_header_historical') }} ihh
        on cast(idh.bill_id as varchar) = cast(ihh.bill_id as varchar)
    join {{ source('omop','concept') }} as c
        on c.concept_code = idh.hcpcs_line_procedure_billed
    where c.domain_id = 'Device'
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
        as varchar) as device_exposure_id,
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
        cast(null as integer) as device_concept_id,
        cast(prdc.service_line_from_date as date) as device_exposure_start_date,
        cast(prdc.service_line_from_date as timestamp) as device_exposure_start_datetime,
        cast(prdc.service_line_to_date as date) as device_exposure_end_date,
        cast(prdc.service_line_to_date as timestamp) as device_exposure_end_datetime,
        32872 as device_type_concept_id,
        cast(null as varchar) as unique_device_id,
        cast(null as varchar) as production_id,
        1 as quantity,
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
        prdc.hcpcs_line_procedure_billed as device_source_value,
        cast(null as integer) as device_source_concept_id,
        cast(null as integer) as unit_concept_id,
        cast(null as varchar) as unit_source_value,
        cast(null as varchar) as unit_source_concept_id
    from {{ source('raw','professional_detail_current') }} prdc
    join {{ source('raw','professional_header_current') }} prhc
        on cast(prdc.bill_id as varchar) = cast(prhc.bill_id as varchar)
    join {{ source('omop','concept') }} as c
        on c.concept_code = prdc.hcpcs_line_procedure_billed
    where c.domain_id = 'Device'
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
        as varchar) as device_exposure_id,
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
        cast(null as integer) as device_concept_id,
        cast(prdh.service_line_from_date as date) as device_exposure_start_date,
        cast(prdh.service_line_from_date as timestamp) as device_exposure_start_datetime,
        cast(prdh.service_line_to_date as date) as device_exposure_end_date,
        cast(prdh.service_line_to_date as timestamp) as device_exposure_end_datetime,
        32872 as device_type_concept_id,
        cast(null as varchar) as unique_device_id,
        cast(null as varchar) as production_id,
        1 as quantity,
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
        prdh.hcpcs_line_procedure_billed as device_source_value,
        cast(null as integer) as device_source_concept_id,
        cast(null as integer) as unit_concept_id,
        cast(null as varchar) as unit_source_value,
        cast(null as varchar) as unit_source_concept_id
    from {{ source('raw','professional_detail_current') }} prdh
    join {{ source('raw','professional_header_current') }} prhh
        on cast(prdh.bill_id as varchar) = cast(prhh.bill_id as varchar)
    join {{ source('omop','concept') }} as c
        on c.concept_code = prdh.hcpcs_line_procedure_billed
    where c.domain_id = 'Device'
        and c.vocabulary_id = 'HCPCS'
)
{% endif %}

{% set cte_list = [] %}
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