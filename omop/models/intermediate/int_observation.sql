select 
    observation_id,
    person_id,
    observation_concept_id,
    observation_date,
    observation_datetime,
    observation_type_concept_id,
    value_as_number,
    value_as_string,
    value_as_concept_id,
    qualifier_concept_id,
    unit_concept_id,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    observation_source_value,
    {{ get_source_concept_ids(
        "observation_source_value",
        domain_id='Measurement',
        vocabulary_id=['ICD9CM', 'ICD10CM', 'CPT4', 'HCPCS']
    ) }} as observation_source_concept_id,
    unit_source_value,
    qualifier_source_value,
    value_source_value,
    observation_event_id,
    obs_event_field_concept_id
from {{ ref('stg_observation') }}