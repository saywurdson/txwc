select
    vdm.visit_detail_id,
    v.person_id,
    cast(v.visit_detail_concept_id as integer) as visit_detail_concept_id,
    v.visit_detail_start_date,
    v.visit_detail_start_datetime,
    v.visit_detail_end_date,
    v.visit_detail_end_datetime,
    cast(v.visit_detail_type_concept_id as integer) as visit_detail_type_concept_id,
    cast(v.provider_id as integer) as provider_id,
    cast(v.care_site_id as integer) as care_site_id,
    vom.visit_occurrence_id,
    v.visit_detail_source_value,
    cast(v.visit_detail_source_concept_id as integer) as visit_detail_source_concept_id,
    cast(v.admitted_from_concept_id as integer) as admitted_from_concept_id,
    v.admitted_from_source_value,
    cast(v.discharged_to_concept_id as integer) as discharged_to_concept_id,
    v.discharged_to_source_value,
    v.preceding_visit_detail_id,
    v.parent_visit_detail_id
from {{ ref('stg_visit_detail') }} v
join {{ ref('int_visit_detail_id_map') }} vdm
    on cast(v.visit_detail_id as integer) = vdm.source_id
left join {{ ref('int_visit_occurrence_id_map') }} vom
    on try_cast(v.visit_occurrence_id as integer) = vom.source_id
where v.visit_detail_start_date is not null
