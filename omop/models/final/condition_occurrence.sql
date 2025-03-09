select
    condition_occurrence_id,
    person_id,
    {{ get_concept_ids(
         "condition_source_concept_id",
         domain_id='Condition',
         vocabulary_id=['ICD9CM', 'ICD10CM'],
         vocabulary_target='SNOMED',
         required_value=0
    ) }} as condition_concept_id,
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
    condition_source_concept_id,
    condition_status_source_value
from {{ ref('int_condition_occurrence') }}
