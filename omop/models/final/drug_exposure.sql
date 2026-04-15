with drug_with_concept as (
    select
        d.*,
        cast({{ get_concept_ids(
             "d.drug_source_concept_id",
             domain_id='Drug',
             vocabulary_id=['NDC', 'HCPCS'],
             vocabulary_target='RxNorm',
             required_value=0
        ) }} as integer) as derived_drug_concept_id
    from {{ ref('int_drug_exposure') }} d
    where d.drug_exposure_start_date is not null
)
select
    row_number() over (order by dwc.drug_exposure_id) as drug_exposure_id,
    dwc.person_id,
    cast(dwc.derived_drug_concept_id as integer) as drug_concept_id,
    dwc.drug_exposure_start_date,
    dwc.drug_exposure_start_datetime,
    dwc.drug_exposure_end_date,
    dwc.drug_exposure_end_datetime,
    dwc.verbatim_end_date,
    cast(dwc.drug_type_concept_id as integer) as drug_type_concept_id,
    dwc.stop_reason,
    dwc.refills,
    dwc.quantity,
    dwc.days_supply,
    dwc.sig,
    {{ get_route_concept_id("dwc.derived_drug_concept_id") }} as route_concept_id,
    dwc.lot_number,
    dwc.provider_id,
    vom.visit_occurrence_id,
    vdm.visit_detail_id,
    dwc.drug_source_value,
    cast(dwc.drug_source_concept_id as integer) as drug_source_concept_id,
    -- Derive route source value (dose form name) from drug concept
    cast({{ get_route_source_value("dwc.derived_drug_concept_id") }} as varchar) as route_source_value,
    -- Derive dose unit from drug_strength table
    cast({{ get_dose_unit_source_value("dwc.derived_drug_concept_id") }} as varchar) as dose_unit_source_value
from drug_with_concept dwc
left join {{ ref('int_visit_occurrence_id_map') }} vom on dwc.visit_occurrence_id = vom.source_id
left join {{ ref('int_visit_detail_id_map') }} vdm on dwc.visit_detail_id = vdm.source_id
