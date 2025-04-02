select
    cast(procedure_occurrence_id as integer) as procedure_occurrence_id,
    cast(person_id as varchar) as person_id,
    cast(procedure_concept_id as varchar) as procedure_concept_id,
    cast(procedure_date as date) as procedure_date,
    cast(procedure_datetime as timestamp) as procedure_datetime,
    cast(procedure_end_date as date) as procedure_end_date,
    cast(procedure_end_datetime as timestamp) as procedure_end_datetime,
    cast(procedure_type_concept_id as varchar) as procedure_type_concept_id,
    cast({{ get_source_concept_ids(
        "modifier_source_value",
        domain_id='Observation',
        vocabulary_id=['HCPCS', 'CPT4'],
        standard_concept='S',
        invalid_reason='is null',
        required_value=0
    ) }} as varchar) as modifier_concept_id,
    cast(quantity as integer) as quantity,
    cast(provider_id as integer) as provider_id,
    cast(visit_occurrence_id as integer) as visit_occurrence_id,
    cast(visit_detail_id as integer) as visit_detail_id,
    cast(procedure_source_value as varchar) as procedure_source_value,
    cast({{ get_source_concept_ids(
        "procedure_source_value",
        domain_id='Procedure',
        vocabulary_id=['ICD9Proc', 'ICD10PCS', 'CPT4', 'HCPCS']
    ) }} as varchar) as procedure_source_concept_id,
    cast(modifier_source_value as varchar) as modifier_source_value
from {{ ref('stg_procedure_occurrence') }}