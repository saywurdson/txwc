select
    condition_occurrence_id,
    person_id,
    condition_concept_id,
    condition_start_date,
    condition_start_datetime,
    condition_end_date,
    condition_end_datetime,
    condition_type_concept_id,
    condition_status_concept_id,
    stop_reason,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    condition_source_value,
    {{ get_source_concept_ids(
        "condition_source_value",
        domain_id='Condition',
        vocabulary_id=['ICD9CM', 'ICD10CM']
    ) }} as condition_source_concept_id,
    condition_status_source_value
from {{ ref('stg_condition_occurrence') }}