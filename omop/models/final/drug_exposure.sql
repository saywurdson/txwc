with drug_with_concept as (
    select
        *,
        cast({{ get_concept_ids(
             "drug_source_concept_id",
             domain_id='Drug',
             vocabulary_id=['NDC', 'HCPCS'],
             vocabulary_target='RxNorm',
             required_value=0
        ) }} as integer) as derived_drug_concept_id
    from {{ ref('int_drug_exposure') }}
    where drug_exposure_start_date is not null
)
select
    row_number() over (order by drug_exposure_id) as drug_exposure_id,
    person_id,
    cast(derived_drug_concept_id as integer) as drug_concept_id,
    drug_exposure_start_date,
    drug_exposure_start_datetime,
    drug_exposure_end_date,
    drug_exposure_end_datetime,
    verbatim_end_date,
    cast(drug_type_concept_id as integer) as drug_type_concept_id,
    stop_reason,
    refills,
    quantity,
    days_supply,
    sig,
    {{ get_route_concept_id("derived_drug_concept_id") }} as route_concept_id,
    lot_number,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    drug_source_value,
    cast(drug_source_concept_id as integer) as drug_source_concept_id,
    -- Derive route source value (dose form name) from drug concept
    cast({{ get_route_source_value("derived_drug_concept_id") }} as varchar) as route_source_value,
    -- Derive dose unit from drug_strength table
    cast({{ get_dose_unit_source_value("derived_drug_concept_id") }} as varchar) as dose_unit_source_value
from drug_with_concept