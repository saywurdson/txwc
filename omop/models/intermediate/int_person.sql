select
    cast(person_id as varchar) as person_id,
    cast(gender_concept_id as integer) as gender_concept_id,
    cast(year_of_birth as integer) as year_of_birth,
    cast(month_of_birth as integer) as month_of_birth,
    cast(day_of_birth as integer) as day_of_birth,
    cast(birth_datetime as timestamp) as birth_datetime,
    cast(race_concept_id as varchar) as race_concept_id,
    cast(ethnicity_concept_id as varchar) as ethnicity_concept_id,
    cast(location_id as integer) as location_id,
    cast(provider_id as integer) as provider_id,
    cast(care_site_id as integer) as care_site_id,
    cast(person_source_value as varchar) as person_source_value,
    cast(gender_source_value as varchar) as gender_source_value,
    cast({{ get_source_concept_ids(
      "gender_source_value",
      domain_id='Gender',
      vocabulary_id=['Gender']
    ) }} as varchar) as gender_source_concept_id,
    cast(race_source_value as varchar) as race_source_value,
    cast(race_source_concept_id as varchar) as race_source_concept_id,
    cast(ethnicity_source_value as varchar) as ethnicity_source_value,
    cast(ethnicity_source_concept_id as varchar) as ethnicity_source_concept_id
from {{ ref('stg_person') }}