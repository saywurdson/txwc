WITH filtered_observations AS (
    SELECT *
    FROM {{ ref('int_observation') }}
    WHERE observation_date IS NOT NULL
)
select
    row_number() over (order by observation_id) as observation_id,
    person_id,
    -- Use SNOMED mapping when available, fall back to staging concept_id for non-ICD observations (e.g. injury date)
    cast(coalesce(
      nullif(cast({{ get_concept_ids(
           "observation_source_concept_id",
           domain_id='Observation',
           vocabulary_id=['ICD9CM', 'ICD10CM', 'CPT4', 'HCPCS'],
           vocabulary_target='SNOMED',
           required_value=0
      ) }} as integer), 0),
      cast(observation_concept_id as integer),
      0
    ) as integer) as observation_concept_id,
    observation_date,
    observation_datetime,
    cast(observation_type_concept_id as integer) as observation_type_concept_id,
    value_as_number,
    value_as_string,
    cast({{ get_concept_ids(
         "observation_source_concept_id",
         domain_id='Observation',
         vocabulary_id=['ICD9CM', 'ICD10CM', 'CPT4', 'HCPCS'],
         vocabulary_target='SNOMED',
         relationship_id='Maps to value',
         required_value=0
    ) }} as integer) as value_as_concept_id,
    cast(qualifier_concept_id as integer) as qualifier_concept_id,
    cast(unit_concept_id as integer) as unit_concept_id,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    observation_source_value,
    cast(observation_source_concept_id as integer) as observation_source_concept_id,
    unit_source_value,
    qualifier_source_value,
    value_source_value,
    observation_event_id,
    cast(obs_event_field_concept_id as integer) as obs_event_field_concept_id
from filtered_observations