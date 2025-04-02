select 
    row_number() over (order by observation_id) as observation_id,
    person_id,
    cast({{ get_concept_ids(
         "observation_source_concept_id",
         domain_id='Observation',
         vocabulary_id=['ICD9CM', 'ICD10CM', 'CPT4', 'HCPCS'],
         vocabulary_target='SNOMED',
         required_value=0
    ) }} as varchar) as observation_concept_id,
    observation_date,
    observation_datetime,
    observation_type_concept_id,
    value_as_number,
    value_as_string,
    cast({{ get_concept_ids(
         "observation_source_concept_id",
         domain_id='Observation',
         vocabulary_id=['ICD9CM', 'ICD10CM', 'CPT4', 'HCPCS'],
         vocabulary_target='SNOMED',
         relationship_id='Maps to value',
         required_value=0
    ) }} as varchar) as value_as_concept_id,
    qualifier_concept_id,
    unit_concept_id,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    observation_source_value,
    observation_source_concept_id,
    unit_source_value,
    qualifier_source_value,
    value_source_value,
    observation_event_id,
    obs_event_field_concept_id
from {{ ref('int_observation') }}