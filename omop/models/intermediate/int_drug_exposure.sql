select 
    cast(drug_exposure_id as integer) as drug_exposure_id,
    cast(person_id as varchar) as person_id,
    cast(drug_concept_id as varchar) as drug_concept_id,
    cast(drug_exposure_start_date as date) as drug_exposure_start_date,
    cast(drug_exposure_start_datetime as timestamp) as drug_exposure_start_datetime,
    cast(drug_exposure_end_date as date) as drug_exposure_end_date,
    cast(drug_exposure_end_datetime as timestamp) as drug_exposure_end_datetime,
    cast(verbatim_end_date as date) as verbatim_end_date,
    cast(drug_type_concept_id as varchar) as drug_type_concept_id,
    cast(stop_reason as varchar) as stop_reason,
    cast(refills as integer) as refills,
    cast(quantity as float) as quantity,
    cast(days_supply as integer) as days_supply,
    cast(sig as varchar) as sig,
    cast(route_concept_id as varchar) as route_concept_id,
    cast(lot_number as varchar) as lot_number,
    cast(provider_id as integer) as provider_id,
    cast(visit_occurrence_id as integer) as visit_occurrence_id,
    cast(visit_detail_id as integer) as visit_detail_id,
    cast(drug_source_value as varchar) as drug_source_value,
    cast({{ get_source_concept_ids(
        "drug_source_value",
        domain_id='Drug',
        vocabulary_id=['NDC', 'HCPCS']
    ) }} as varchar) as drug_source_concept_id,
    cast(route_source_value as integer) as route_source_value,
    cast(dose_unit_source_value as integer) as dose_unit_source_value
from {{ ref('stg_drug_exposure') }}