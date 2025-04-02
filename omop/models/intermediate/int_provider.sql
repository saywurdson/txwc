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
from {{ ref('stg_provider') }}