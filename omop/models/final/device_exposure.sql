select 
    row_number() over (order by device_exposure_id) as device_exposure_id,
    person_id,
    cast({{ get_concept_ids(
         "device_source_concept_id",
         domain_id='Device',
         vocabulary_id=['HCPCS'],
         vocabulary_target='SNOMED',
         required_value=0
    ) }} as varchar) as device_concept_id,
    device_exposure_start_date,
    device_exposure_start_datetime,
    device_exposure_end_date,
    device_exposure_end_datetime,
    device_type_concept_id,
    unique_device_id,
    production_id,
    quantity,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    device_source_value,
    device_source_concept_id,
    unit_concept_id,
    unit_source_value,
    unit_source_concept_id
from {{ ref('int_device_exposure') }}