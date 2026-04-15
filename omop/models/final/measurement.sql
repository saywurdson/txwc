select
    row_number() over (order by m.measurement_id) as measurement_id,
    m.person_id,
    cast({{ get_concept_ids(
         "m.measurement_source_concept_id",
         domain_id='Measurement',
         vocabulary_id=['ICD9CM', 'ICD10CM', 'CPT4', 'HCPCS'],
         vocabulary_target='SNOMED',
         required_value=0
    ) }} as integer) as measurement_concept_id,
    m.measurement_date,
    m.measurement_datetime,
    m.measurement_time,
    cast(m.measurement_type_concept_id as integer) as measurement_type_concept_id,
    cast(m.operator_concept_id as integer) as operator_concept_id,
    m.value_as_number,
    cast(m.value_as_concept_id as integer) as value_as_concept_id,
    cast(m.unit_concept_id as integer) as unit_concept_id,
    m.range_low,
    m.range_high,
    m.provider_id,
    vom.visit_occurrence_id,
    vdm.visit_detail_id,
    m.measurement_source_value,
    cast(m.measurement_source_concept_id as integer) as measurement_source_concept_id,
    m.unit_source_value,
    cast(m.unit_source_concept_id as integer) as unit_source_concept_id,
    m.value_source_value,
    m.measurement_event_id,
    cast(m.meas_event_field_concept_id as integer) as meas_event_field_concept_id
from {{ ref('int_measurement') }} m
left join {{ ref('int_visit_occurrence_id_map') }} vom on m.visit_occurrence_id = vom.source_id
left join {{ ref('int_visit_detail_id_map') }} vdm on m.visit_detail_id = vdm.source_id
where m.measurement_date is not null
