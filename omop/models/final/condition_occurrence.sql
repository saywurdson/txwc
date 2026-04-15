select
    row_number() over (order by c.condition_occurrence_id) as condition_occurrence_id,
    c.person_id,
    cast({{ get_concept_ids(
         "c.condition_source_concept_id",
         domain_id='Condition',
         vocabulary_id=['ICD9CM', 'ICD10CM'],
         vocabulary_target='SNOMED',
         required_value=0
    ) }} as integer) as condition_concept_id,
    c.condition_start_date,
    c.condition_start_datetime,
    c.condition_end_date,
    c.condition_end_datetime,
    cast(c.condition_type_concept_id as integer) as condition_type_concept_id,
    cast(c.condition_status_concept_id as integer) as condition_status_concept_id,
    c.stop_reason,
    c.provider_id,
    vom.visit_occurrence_id,
    vdm.visit_detail_id,
    c.condition_source_value,
    cast(c.condition_source_concept_id as integer) as condition_source_concept_id,
    c.condition_status_source_value
from {{ ref('int_condition_occurrence') }} c
left join {{ ref('int_visit_occurrence_id_map') }} vom on c.visit_occurrence_id = vom.source_id
left join {{ ref('int_visit_detail_id_map') }} vdm on c.visit_detail_id = vdm.source_id
where c.condition_start_date is not null
