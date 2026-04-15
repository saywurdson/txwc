select
    row_number() over (order by p.procedure_occurrence_id) as procedure_occurrence_id,
    p.person_id,
    cast({{ get_concept_ids(
        "p.procedure_source_concept_id",
         domain_id='Procedure',
         vocabulary_id=['ICD9Proc', 'ICD10PCS', 'CPT4', 'HCPCS'],
         vocabulary_target='SNOMED',
         required_value=0
    ) }} as integer) as procedure_concept_id,
    p.procedure_date,
    p.procedure_datetime,
    p.procedure_end_date,
    p.procedure_end_datetime,
    cast(p.procedure_type_concept_id as integer) as procedure_type_concept_id,
    cast(p.modifier_concept_id as integer) as modifier_concept_id,
    p.quantity,
    p.provider_id,
    vom.visit_occurrence_id,
    vdm.visit_detail_id,
    p.procedure_source_value,
    cast(p.procedure_source_concept_id as integer) as procedure_source_concept_id,
    p.modifier_source_value
from {{ ref('int_procedure_occurrence') }} p
left join {{ ref('int_visit_occurrence_id_map') }} vom on p.visit_occurrence_id = vom.source_id
left join {{ ref('int_visit_detail_id_map') }} vdm on p.visit_detail_id = vdm.source_id
where p.procedure_date is not null
