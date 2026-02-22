select
    row_number() over (order by visit_occurrence_id) as visit_occurrence_id,
    person_id,
    cast({{ get_concept_ids(
         "visit_source_concept_id",
         domain_id='Visit',
         vocabulary_id=['UB04 Typ bill'],
         vocabulary_target='CMS Place of Service',
         required_value=0
    ) }} as integer) as visit_concept_id,
    visit_start_date,
    visit_start_datetime,
    visit_end_date,
    visit_end_datetime,
    cast(visit_type_concept_id as integer) as visit_type_concept_id,
    provider_id,
    care_site_id,
    visit_source_value,
    cast(visit_source_concept_id as integer) as visit_source_concept_id,
    cast(admitted_from_concept_id as integer) as admitted_from_concept_id,
    admitted_from_source_value,
    cast(discharged_to_concept_id as integer) as discharged_to_concept_id,
    discharged_to_source_value,
    preceding_visit_occurrence_id
from {{ ref('int_visit_occurrence') }}
where visit_start_date is not null