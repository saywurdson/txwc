select
    procedure_occurrence_id,
    person_id,
    procedure_concept_id,
    procedure_date,
    procedure_datetime,
    procedure_end_date,
    procedure_end_datetime,
    procedure_type_concept_id,
    {{ get_source_concept_ids(
        "modifier_source_value",
        domain_id='Observation',
        vocabulary_id=['HCPCS', 'CPT4'],
        standard_concept='S',
        invalid_reason='is null',
        required_value=0
    ) }} as modifier_concept_id,
    quantity,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    procedure_source_value,
    {{ get_source_concept_ids(
        "procedure_source_value",
        domain_id='Procedure',
        vocabulary_id=['ICD9Proc', 'ICD10PCS', 'CPT4', 'HCPCS']
    ) }} as procedure_source_concept_id,
    modifier_source_value
from {{ ref('stg_procedure_occurrence') }}