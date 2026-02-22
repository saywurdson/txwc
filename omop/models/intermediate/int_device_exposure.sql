select 
    cast(device_exposure_id as integer) as device_exposure_id,
    cast(person_id as integer) as person_id,
    cast(device_concept_id as varchar) as device_concept_id,
    cast(device_exposure_start_date as date) as device_exposure_start_date,
    cast(device_exposure_start_datetime as timestamp) as device_exposure_start_datetime,
    cast(device_exposure_end_date as date) as device_exposure_end_date,
    cast(device_exposure_end_datetime as timestamp) as device_exposure_end_datetime,
    cast(device_type_concept_id as varchar) as device_type_concept_id,
    cast(unique_device_id as varchar) as unique_device_id,
    cast(production_id as varchar) as production_id,
    cast(quantity as integer) as quantity,
    cast(provider_id as integer) as provider_id,
    cast(visit_occurrence_id as integer) as visit_occurrence_id,
    cast(visit_detail_id as integer) as visit_detail_id,
    cast(device_source_value as varchar) as device_source_value,
    cast({{ get_source_concept_ids(
        "device_source_value",
        domain_id='Device',
        vocabulary_id=['HCPCS']
    ) }} as varchar) as device_source_concept_id,
    cast(unit_concept_id as varchar) as unit_concept_id,
    cast(unit_source_value as varchar) as unit_source_value,
    cast(unit_source_concept_id as varchar) as unit_source_concept_id
from {{ ref('stg_device_exposure') }}