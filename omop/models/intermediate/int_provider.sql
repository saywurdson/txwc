-- Deduplicate providers by provider_id, keeping the most complete record
-- Providers may appear with different care_site_ids when working at multiple facilities
with ranked_providers as (
    select
        provider_id,
        provider_name,
        npi,
        dea,
        specialty_concept_id,
        care_site_id,
        year_of_birth,
        gender_concept_id,
        provider_source_value,
        specialty_source_value,
        gender_source_value,
        gender_source_concept_id,
        row_number() over (
            partition by provider_id
            order by
                -- Prefer records with valid NPI (10 digits)
                case when regexp_matches(npi, '^[0-9]{10}$') then 0 else 1 end,
                -- Then prefer records with specialty
                case when specialty_concept_id is not null and specialty_concept_id != 0 then 0 else 1 end,
                -- Then prefer records with care_site_id
                case when care_site_id is not null then 0 else 1 end,
                -- Then prefer longer provider names (more complete)
                length(coalesce(provider_name, '')) desc
        ) as rn
    from {{ ref('stg_provider') }}
)
select
    cast(provider_id as integer) as provider_id,
    cast(provider_name as varchar) as provider_name,
    cast(npi as varchar) as npi,
    cast(dea as varchar) as dea,
    cast(specialty_concept_id as varchar) as specialty_concept_id,
    cast(care_site_id as integer) as care_site_id,
    cast(year_of_birth as integer) as year_of_birth,
    cast(gender_concept_id as varchar) as gender_concept_id,
    cast(provider_source_value as varchar) as provider_source_value,
    cast(specialty_source_value as varchar) as specialty_source_value,
    cast({{ get_source_concept_ids(
        "specialty_source_value",
        domain_id=['Visit', 'Provider'],
        vocabulary_id=['NUCC', 'Medicare Specialty']
    ) }} as varchar) as specialty_source_concept_id,
    cast(gender_source_value as integer) as gender_source_value,
    cast(gender_source_concept_id as varchar) as gender_source_concept_id
from ranked_providers
where rn = 1