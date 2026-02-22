select
    row_number() over (order by visit_detail_id) as visit_detail_id,
    person_id,
    cast(visit_detail_concept_id as integer) as visit_detail_concept_id,
    visit_detail_start_date,
    visit_detail_start_datetime,
    visit_detail_end_date,
    visit_detail_end_datetime,
    cast(visit_detail_type_concept_id as integer) as visit_detail_type_concept_id,
    cast(provider_id as integer) as provider_id,
    cast(care_site_id as integer) as care_site_id,
    visit_occurrence_id,
    visit_detail_source_value,
    cast(visit_detail_source_concept_id as integer) as visit_detail_source_concept_id,
    cast(admitted_from_concept_id as integer) as admitted_from_concept_id,
    admitted_from_source_value,
    cast(discharged_to_concept_id as integer) as discharged_to_concept_id,
    discharged_to_source_value,
    preceding_visit_detail_id,
    parent_visit_detail_id
from {{ ref('stg_visit_detail') }}
where visit_detail_start_date is not null
