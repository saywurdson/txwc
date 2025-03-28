select
    provider_id,
    provider_name,
    npi,
    dea,
    cast({{ get_concept_ids(
         "specialty_source_concept_id",
         domain_id=['Visit', 'Provider'],
         vocabulary_id=['NUCC'],
         vocabulary_target=['NUCC', 'Medicare Specialty'],
         required_value=0
    ) }} as integer) as specialty_concept_id,
    care_site_id,
    year_of_birth,
    gender_concept_id,
    provider_source_value,
    specialty_source_value,
    specialty_source_concept_id,
    gender_source_value,
    gender_source_concept_id
from {{ ref('int_provider') }}