{% macro get_route_concept_id(drug_concept_id) %}
(
  select coalesce(
    -- First try: explicit route from dose form relationship chain
    (
      select cast(route.concept_id as integer)
      from {{ source('omop','concept_relationship') }} cr1  -- Drug -> Dose Form
      join {{ source('omop','concept_relationship') }} cr2
        on cr1.concept_id_2 = cr2.concept_id_1  -- Dose Form -> Route
      join {{ source('omop','concept') }} route
        on cr2.concept_id_2 = route.concept_id
      where cr1.concept_id_1 = {{ drug_concept_id }}
        and cr1.relationship_id = 'RxNorm has dose form'
        and cr2.relationship_id = 'Has route of admin'
        and route.domain_id = 'Route'
        and route.standard_concept = 'S'
      limit 1
    ),
    -- Fallback: infer route from dose form name
    (
      select cast(case
        when df.concept_name ilike '%Oral%' or df.concept_name ilike '%Chew%' or df.concept_name ilike '%Lozenge%'
          or df.concept_name ilike '%Tablet%' or df.concept_name ilike '%Capsule%' then 4132161  -- Oral
        when df.concept_name ilike '%Intramuscular%' then 4302612  -- Intramuscular
        when df.concept_name ilike '%Intravenous%' then 4171047  -- Intravenous
        when df.concept_name ilike '%Subcutaneous%' then 4142048  -- Subcutaneous
        when df.concept_name ilike '%Inject%' or df.concept_name ilike '%Syringe%'
          or df.concept_name ilike '%Prefilled%' then 4171047  -- Intravenous (default for injectables)
        when df.concept_name ilike '%Topical%' or df.concept_name ilike '%Cream%'
          or df.concept_name ilike '%Ointment%' or df.concept_name ilike '%Lotion%'
          or df.concept_name ilike '%Shampoo%' or df.concept_name ilike '%Soap%' then 4263689  -- Topical
        when df.concept_name ilike '%Ophthalmic%' or df.concept_name ilike '%Eye%' then 4184451  -- Ophthalmic
        when df.concept_name ilike '%Nasal%' then 4262914  -- Nasal
        when df.concept_name ilike '%Inhal%' or df.concept_name ilike '%Nebul%' then 40486069  -- Respiratory tract
        when df.concept_name ilike '%Rectal%' or df.concept_name ilike '%Enema%' then 4290759  -- Rectal
        when df.concept_name ilike '%Vaginal%' then 4057765  -- Vaginal
        when df.concept_name ilike '%Sublingual%' then 4292110  -- Sublingual
        when df.concept_name ilike '%Buccal%' then 4181897  -- Buccal
        when df.concept_name ilike '%Transdermal%' or df.concept_name ilike '%Patch%' then 4262099  -- Transdermal
        when df.concept_name ilike '%Otic%' or df.concept_name ilike '%Ear%' then 4023156  -- Otic
        when df.concept_name ilike '%Implant%' then 4266683  -- Implant
        else null
      end as integer)
      from {{ source('omop','concept_relationship') }} cr
      join {{ source('omop','concept') }} df on cr.concept_id_2 = df.concept_id
      where cr.concept_id_1 = {{ drug_concept_id }}
        and cr.relationship_id = 'RxNorm has dose form'
      limit 1
    ),
    cast(null as integer)  -- If no route can be determined
  ) as route_concept_id
)
{% endmacro %}
