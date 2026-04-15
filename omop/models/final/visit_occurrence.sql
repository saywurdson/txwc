select
    m.visit_occurrence_id,
    v.person_id,
    cast({{ get_concept_ids(
         "v.visit_source_concept_id",
         domain_id='Visit',
         vocabulary_id=['UB04 Typ bill'],
         vocabulary_target='CMS Place of Service',
         required_value=0
    ) }} as integer) as visit_concept_id,
    v.visit_start_date,
    v.visit_start_datetime,
    v.visit_end_date,
    v.visit_end_datetime,
    cast(v.visit_type_concept_id as integer) as visit_type_concept_id,
    v.provider_id,
    v.care_site_id,
    v.visit_source_value,
    cast(v.visit_source_concept_id as integer) as visit_source_concept_id,
    cast(v.admitted_from_concept_id as integer) as admitted_from_concept_id,
    v.admitted_from_source_value,
    cast(v.discharged_to_concept_id as integer) as discharged_to_concept_id,
    v.discharged_to_source_value,
    v.preceding_visit_occurrence_id
from {{ ref('int_visit_occurrence') }} v
join {{ ref('int_visit_occurrence_id_map') }} m
    on cast(v.visit_occurrence_id as integer) = m.source_id
where v.visit_start_date is not null
