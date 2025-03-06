select
    visit_occurrence_id,
    person_id,
    visit_concept_id,
    visit_start_date,
    visit_start_datetime,
    visit_end_date,
    visit_end_datetime,
    visit_type_concept_id,
    provider_id,
    care_site_id,
    provider_id,
    care_site_id,
    visit_source_value,
    {{ get_source_concept_ids(
      "visit_source_value",
      domain_id='Visit',
      vocabulary_id=['UB04 Typ bill']
    ) }} as visit_source_concept_id,
    admitted_from_source_value,
    discharged_to_concept_id,
    discharged_to_source_value,
    preceding_visit_occurrence_id
from {{ ref('stg_visit_occurrence') }}