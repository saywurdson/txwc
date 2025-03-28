select 
    cast(observation_id as integer) as observation_id,
    cast(person_id as varchar) as person_id,
    cast(observation_concept_id as integer) as observation_concept_id,
    cast(observation_date as date) as observation_date,
    cast(observation_datetime as timestamp) as observation_datetime,
    cast(observation_type_concept_id as integer) as observation_type_concept_id,
    cast(value_as_number as float) as value_as_number,
    cast(value_as_string as varchar) as value_as_string,
    cast(value_as_concept_id as integer) as value_as_concept_id,
    cast(qualifier_concept_id as integer) as qualifier_concept_id,
    cast(unit_concept_id as integer) as unit_concept_id,
    cast(provider_id as integer) as provider_id,
    cast(visit_occurrence_id as integer) as visit_occurrence_id,
    cast(visit_detail_id as integer) as visit_detail_id,
    cast(observation_source_value as varchar) as observation_source_value,
    cast({{ get_source_concept_ids(
        "observation_source_value",
        domain_id='Observation',
        vocabulary_id=['ICD9CM', 'ICD10CM', 'CPT4', 'HCPCS']
    ) }} as integer) as observation_source_concept_id,
    cast(unit_source_value as varchar) as unit_source_value,
    cast(qualifier_source_value as varchar) as qualifier_source_value,
    cast(value_source_value as varchar) as value_source_value,
    cast(observation_event_id as integer) as observation_event_id,
    cast(obs_event_field_concept_id as integer) as obs_event_field_concept_id
from {{ ref('stg_observation') }}