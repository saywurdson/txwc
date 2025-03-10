select
    row_number() over (order by procedure_occurrence_id) as procedure_occurrence_id,
    person_id,
    {{ get_concept_ids(
        "procedure_source_concept_id",
         domain_id='Procedure',
         vocabulary_id=['ICD9Proc', 'ICD10PCS', 'CPT4', 'HCPCS'],
         vocabulary_target='SNOMED',
         required_value=0
    ) }} as procedure_concept_id,
    procedure_date,
    procedure_datetime,
    procedure_end_date,
    procedure_end_datetime,
    procedure_type_concept_id,
    modifier_concept_id,
    quantity,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    procedure_source_value,
    procedure_source_concept_id,
    modifier_source_value
from {{ ref('int_procedure_occurrence') }}