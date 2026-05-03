{% macro get_route_source_value(drug_concept_id) %}
(
  select df.concept_name
  from {{ source('omop','concept_relationship') }} cr
  join {{ source('omop','concept') }} df on cr.concept_id_2 = df.concept_id
  where cr.concept_id_1 = {{ drug_concept_id }}
    and cr.relationship_id = 'RxNorm has dose form'
  order by df.concept_id  -- determinism: a drug may have multiple dose forms
  limit 1
)
{% endmacro %}
