WITH filtered_observations AS (
    SELECT *
    FROM {{ ref('int_observation') }}
    WHERE observation_date IS NOT NULL
)
select
    row_number() over (order by o.observation_id) as observation_id,
    o.person_id,
    -- Use SNOMED mapping when available, fall back to staging concept_id for non-ICD observations (e.g. injury date)
    cast(coalesce(
      nullif(cast({{ get_concept_ids(
           "o.observation_source_concept_id",
           domain_id='Observation',
           vocabulary_id=['ICD9CM', 'ICD10CM', 'CPT4', 'HCPCS'],
           vocabulary_target='SNOMED',
           required_value=0
      ) }} as integer), 0),
      cast(o.observation_concept_id as integer),
      0
    ) as integer) as observation_concept_id,
    o.observation_date,
    o.observation_datetime,
    cast(o.observation_type_concept_id as integer) as observation_type_concept_id,
    o.value_as_number,
    o.value_as_string,
    cast({{ get_concept_ids(
         "o.observation_source_concept_id",
         domain_id='Observation',
         vocabulary_id=['ICD9CM', 'ICD10CM', 'CPT4', 'HCPCS'],
         vocabulary_target='SNOMED',
         relationship_id='Maps to value',
         required_value=0
    ) }} as integer) as value_as_concept_id,
    cast(o.qualifier_concept_id as integer) as qualifier_concept_id,
    cast(o.unit_concept_id as integer) as unit_concept_id,
    o.provider_id,
    vom.visit_occurrence_id,
    vdm.visit_detail_id,
    o.observation_source_value,
    cast(o.observation_source_concept_id as integer) as observation_source_concept_id,
    o.unit_source_value,
    o.qualifier_source_value,
    o.value_source_value,
    o.observation_event_id,
    cast(o.obs_event_field_concept_id as integer) as obs_event_field_concept_id
from filtered_observations o
left join {{ ref('int_visit_occurrence_id_map') }} vom on o.visit_occurrence_id = vom.source_id
left join {{ ref('int_visit_detail_id_map') }} vdm on o.visit_detail_id = vdm.source_id
