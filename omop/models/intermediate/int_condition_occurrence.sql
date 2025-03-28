select
    cast(condition_occurrence_id as integer) as condition_occurrence_id,
    cast(person_id as varchar) as person_id,
    cast(condition_concept_id as integer) as condition_concept_id,
    cast(condition_start_date as date) as condition_start_date,
    cast(condition_start_datetime as timestamp) as condition_start_datetime,
    cast(condition_end_date as date) as condition_end_date,
    cast(condition_end_datetime as timestamp) as condition_end_datetime,
    cast(condition_type_concept_id as integer) as condition_type_concept_id,
    cast(condition_status_concept_id as integer) as condition_status_concept_id,
    cast(stop_reason as varchar) as stop_reason,
    cast(provider_id as integer) as provider_id,
    cast(visit_occurrence_id as integer) as visit_occurrence_id,
    cast(visit_detail_id as integer) as visit_detail_id,
    cast(condition_source_value as varchar) as condition_source_value,
    cast({{ get_source_concept_ids(
        "condition_source_value",
        domain_id='Condition',
        vocabulary_id=['ICD9CM', 'ICD10CM']
    ) }} as integer) as condition_source_concept_id,
    cast(condition_status_source_value as varchar) as condition_status_source_value
from {{ ref('stg_condition_occurrence') }}