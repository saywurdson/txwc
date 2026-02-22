select 
    cast(measurement_id as integer) as measurement_id,
    cast(person_id as integer) as person_id,
    cast(measurement_concept_id as varchar) as measurement_concept_id,
    cast(measurement_date as date) as measurement_date,
    cast(measurement_datetime as timestamp) as measurement_datetime,
    cast(measurement_time as varchar) as measurement_time,
    cast(measurement_type_concept_id as varchar) as measurement_type_concept_id,
    cast(operator_concept_id as varchar) as operator_concept_id,
    cast(value_as_number as float) as value_as_number,
    cast(value_as_concept_id as varchar) as value_as_concept_id,
    cast(unit_concept_id as varchar) as unit_concept_id,
    cast(range_low as float) as range_low,
    cast(range_high as float) as range_high,
    cast(provider_id as integer) as provider_id,
    cast(visit_occurrence_id as integer) as visit_occurrence_id,
    cast(visit_detail_id as integer) as visit_detail_id,
    cast(measurement_source_value as varchar) as measurement_source_value,
    cast({{ get_source_concept_ids(
        "measurement_source_value",
        domain_id='Measurement',
        vocabulary_id=['ICD9CM', 'ICD10CM', 'CPT4', 'HCPCS']
    ) }} as varchar) as measurement_source_concept_id,
    cast(unit_source_value as varchar) as unit_source_value,
    cast(unit_source_concept_id as varchar) as unit_source_concept_id,
    cast(value_source_value as varchar) as value_source_value,
    cast(measurement_event_id as integer) as measurement_event_id,
    cast(meas_event_field_concept_id as varchar) as meas_event_field_concept_id
from {{ ref('stg_measurement') }}
where measurement_id is not null