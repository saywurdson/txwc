select
    cast(visit_occurrence_id as integer) as visit_occurrence_id,
    cast(person_id as varchar) as person_id,
    cast(visit_concept_id as varchar) as visit_concept_id,
    cast(visit_start_date as date) as visit_start_date,
    cast(visit_start_datetime as timestamp) as visit_start_datetime,
    cast(visit_end_date as date) as visit_end_date,
    cast(visit_end_datetime as timestamp) as visit_end_datetime,
    cast(visit_type_concept_id as varchar) as visit_type_concept_id,
    cast(provider_id as integer) as provider_id,
    cast(care_site_id as integer) as care_site_id,
    cast(visit_source_value as varchar) as visit_source_value,
    cast({{ get_source_concept_ids(
      "visit_source_value",
      domain_id='Visit',
      vocabulary_id=['UB04 Typ bill']
    ) }} as varchar) as visit_source_concept_id,
    cast(admitted_from_concept_id as varchar) as admitted_from_concept_id,
    cast(admitted_from_source_value as varchar) as admitted_from_source_value,
    cast(discharged_to_concept_id as varchar) as discharged_to_concept_id,
    cast(discharged_to_source_value as varchar) as discharged_to_source_value,
    cast(preceding_visit_occurrence_id as integer) as preceding_visit_occurrence_id
from {{ ref('stg_visit_occurrence') }}