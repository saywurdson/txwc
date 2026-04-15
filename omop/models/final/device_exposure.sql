select
    row_number() over (order by d.device_exposure_id) as device_exposure_id,
    d.person_id,
    cast({{ get_concept_ids(
         "d.device_source_concept_id",
         domain_id='Device',
         vocabulary_id=['HCPCS'],
         vocabulary_target='SNOMED',
         required_value=0
    ) }} as integer) as device_concept_id,
    d.device_exposure_start_date,
    d.device_exposure_start_datetime,
    d.device_exposure_end_date,
    d.device_exposure_end_datetime,
    cast(d.device_type_concept_id as integer) as device_type_concept_id,
    d.unique_device_id,
    d.production_id,
    d.quantity,
    d.provider_id,
    vom.visit_occurrence_id,
    vdm.visit_detail_id,
    d.device_source_value,
    cast(d.device_source_concept_id as integer) as device_source_concept_id,
    cast(d.unit_concept_id as integer) as unit_concept_id,
    d.unit_source_value,
    cast(d.unit_source_concept_id as integer) as unit_source_concept_id
from {{ ref('int_device_exposure') }} d
left join {{ ref('int_visit_occurrence_id_map') }} vom on d.visit_occurrence_id = vom.source_id
left join {{ ref('int_visit_detail_id_map') }} vdm on d.visit_detail_id = vdm.source_id
where d.device_exposure_start_date is not null
