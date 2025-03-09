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
        cast(
            hash(
                concat_ws(
                '||',
                rendering_bill_provider_last,
                coalesce(rendering_bill_provider_first, ''),
                coalesce(rendering_bill_provider_middle, ''),
                rendering_bill_provider_state_1,
                rendering_bill_provider_4
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as provider_id,
        concat(
            rendering_bill_provider_last, 
            case 
                when rendering_bill_provider_first is not null 
                then concat(', ', rendering_bill_provider_first)
                else ''
            end,
            case 
                when rendering_bill_provider_middle is not null 
                then concat(' ', rendering_bill_provider_middle)
                else ''
            end
        ) as provider_name,
        rendering_bill_provider_4 as npi,
        cast(null as varchar) as dea,
        cast(null as integer) as specialty_concept_id,
        cast(
            hash(
                concat_ws(
                '||',
                billing_provider_last_name
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as care_site_id,
        cast(null as integer) as year_of_birth,
        cast(null as integer) as gender_concept_id,
        rendering_bill_provider_state_1 as provider_source_value,
        rendering_bill_provider as specialty_source_value,
        cast(null as integer) as specialty_source_concept_id,
        cast(null as varchar) as gender_source_value,
        cast(null as integer) as gender_source_concept_id
    from {{ source('raw', 'institutional_header_current') }}
)
{% endif %}

{% if exists_i_historical %}
{% if exists_i_current %}, {% endif %}
institutional_header_historical as (
    select distinct
        cast(
            hash(
                concat_ws(
                '||',
                rendering_bill_provider_last,
                coalesce(rendering_bill_provider_first, ''),
                coalesce(rendering_bill_provider_middle, ''),
                rendering_bill_provider_state_1,
                rendering_bill_provider_4
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as provider_id,
        concat(
            rendering_bill_provider_last, 
            case 
                when rendering_bill_provider_first is not null 
                then concat(', ', rendering_bill_provider_first)
                else ''
            end,
            case 
                when rendering_bill_provider_middle is not null 
                then concat(' ', rendering_bill_provider_middle)
                else ''
            end
        ) as provider_name,
        rendering_bill_provider_4 as npi,
        cast(null as varchar) as dea,
        cast(null as integer) as specialty_concept_id,
        cast(
            hash(
                concat_ws(
                '||',
                billing_provider_last_name
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as care_site_id,
        cast(null as integer) as year_of_birth,
        cast(null as integer) as gender_concept_id,
        rendering_bill_provider_state_1 as provider_source_value,
        rendering_bill_provider as specialty_source_value,
        cast(null as integer) as specialty_source_concept_id,
        cast(null as varchar) as gender_source_value,
        cast(null as integer) as gender_source_concept_id
    from {{ source('raw', 'institutional_header_historical') }}
)
{% endif %}

{% if exists_pr_historical %}
{% if exists_i_historical %}, {% endif %}
professional_header_historical as (
    select distinct
        cast(
            hash(
                concat_ws(
                '||',
                rendering_bill_provider_last,
                coalesce(rendering_bill_provider_first, ''),
                coalesce(rendering_bill_provider_middle, ''),
                rendering_bill_provider_state_1,
                rendering_bill_provider_4
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as provider_id,
        concat(
            rendering_bill_provider_last, 
            case 
                when rendering_bill_provider_first is not null 
                then concat(', ', rendering_bill_provider_first)
                else ''
            end,
            case 
                when rendering_bill_provider_middle is not null 
                then concat(' ', rendering_bill_provider_middle)
                else ''
            end
        ) as provider_name,
        rendering_bill_provider_4 as npi,
        cast(null as varchar) as dea,
        cast(null as integer) as specialty_concept_id,
        cast(
            hash(
                concat_ws(
                '||',
                billing_provider_last_name,
                facility_primary_address
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as care_site_id,
        cast(null as integer) as year_of_birth,
        cast(null as integer) as gender_concept_id,
        rendering_bill_provider_state_1 as provider_source_value,
        rendering_bill_provider as specialty_source_value,
        cast(null as integer) as specialty_source_concept_id,
        cast(null as varchar) as gender_source_value,
        cast(null as integer) as gender_source_concept_id
    from {{ source('raw', 'professional_header_historical') }}
)
{% endif %}

{% if exists_pr_current %}
{% if exists_pr_historical %}, {% endif %}
professional_header_current as (
    select distinct
        cast(
            hash(
                concat_ws(
                '||',
                rendering_bill_provider_last,
                coalesce(rendering_bill_provider_first, ''),
                coalesce(rendering_bill_provider_middle, ''),
                rendering_bill_provider_state_1,
                rendering_bill_provider_4
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as provider_id,
        concat(
            rendering_bill_provider_last, 
            case 
                when rendering_bill_provider_first is not null 
                then concat(', ', rendering_bill_provider_first)
                else ''
            end,
            case 
                when rendering_bill_provider_middle is not null 
                then concat(' ', rendering_bill_provider_middle)
                else ''
            end
        ) as provider_name,
        rendering_bill_provider_4 as npi,
        cast(null as varchar) as dea,
        cast(null as integer) as specialty_concept_id,
        cast(
            hash(
                concat_ws(
                '||',
                billing_provider_last_name,
                facility_primary_address
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as care_site_id,
        cast(null as integer) as year_of_birth,
        cast(null as integer) as gender_concept_id,
        rendering_bill_provider_state_1 as provider_source_value,
        rendering_bill_provider as specialty_source_value,
        cast(null as integer) as specialty_source_concept_id,
        cast(null as varchar) as gender_source_value,
        cast(null as integer) as gender_source_concept_id
    from {{ source('raw', 'professional_header_current') }}
)
{% endif %}

{% if exists_ph_current %}
{% if exists_pr_current %}, {% endif %}
pharmacy_header_current as (
    select distinct
        cast(
            hash(
                concat_ws(
                '||',
                rendering_bill_provider_last,
                coalesce(rendering_bill_provider_first, ''),
                coalesce(rendering_bill_provider_middle, ''),
                rendering_bill_provider_state_1,
                rendering_bill_provider_4
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as provider_id,
        concat(
            rendering_bill_provider_last, 
            case 
                when rendering_bill_provider_first is not null 
                then concat(', ', rendering_bill_provider_first)
                else ''
            end,
            case 
                when rendering_bill_provider_middle is not null 
                then concat(' ', rendering_bill_provider_middle)
                else ''
            end
        ) as provider_name,
        rendering_bill_provider_4 as npi,
        cast(null as varchar) as dea,
        cast(null as integer) as specialty_concept_id,
        cast(
            hash(
                concat_ws(
                '||',
                billing_provider_last_name,
                billing_provider_fein
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as care_site_id,
        cast(null as integer) as year_of_birth,
        cast(null as integer) as gender_concept_id,
        rendering_bill_provider_state_1 as provider_source_value,
        rendering_bill_provider as specialty_source_value,
        cast(null as integer) as specialty_source_concept_id,
        cast(null as varchar) as gender_source_value,
        cast(null as integer) as gender_source_concept_id
    from {{ source('raw', 'pharmacy_header_current') }}
)
{% endif %}

{% if exists_ph_historical %}
{% if exists_ph_current %}, {% endif %}
pharmacy_header_historical as (
    select distinct
        cast(
            hash(
                concat_ws(
                '||',
                rendering_bill_provider_last,
                coalesce(rendering_bill_provider_first, ''),
                coalesce(rendering_bill_provider_middle, ''),
                rendering_bill_provider_state_1,
                rendering_bill_provider_4
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as provider_id,
        concat(
            rendering_bill_provider_last, 
            case 
                when rendering_bill_provider_first is not null 
                then concat(', ', rendering_bill_provider_first)
                else ''
            end,
            case 
                when rendering_bill_provider_middle is not null 
                then concat(' ', rendering_bill_provider_middle)
                else ''
            end
        ) as provider_name,
        rendering_bill_provider_4 as npi,
        cast(null as varchar) as dea,
        cast(null as integer) as specialty_concept_id,
        cast(
            hash(
                concat_ws(
                '||',
                billing_provider_last_name,
                billing_provider_fein
                )
            , 'xxhash64'
            ) % 1000000000
        as varchar) as care_site_id,
        cast(null as integer) as year_of_birth,
        cast(null as integer) as gender_concept_id,
        rendering_bill_provider_state_1 as provider_source_value,
        rendering_bill_provider as specialty_source_value,
        cast(null as integer) as specialty_source_concept_id,
        cast(null as varchar) as gender_source_value,
        cast(null as integer) as gender_source_concept_id
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