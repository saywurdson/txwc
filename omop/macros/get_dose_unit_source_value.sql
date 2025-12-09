{% macro get_dose_unit_source_value(drug_concept_id) %}
(
  select
    -- Build dose string from drug_strength: amount or numerator/denominator
    case
      when ds.amount_value is not null then
        concat(
          cast(ds.amount_value as varchar),
          ' ',
          coalesce(amount_unit.concept_name, '')
        )
      when ds.numerator_value is not null then
        concat(
          cast(ds.numerator_value as varchar),
          ' ',
          coalesce(numer_unit.concept_name, ''),
          case
            when ds.denominator_value is not null then
              concat('/', cast(ds.denominator_value as varchar), ' ', coalesce(denom_unit.concept_name, ''))
            else ''
          end
        )
      else null
    end
  from {{ source('omop','drug_strength') }} ds
  left join {{ source('omop','concept') }} amount_unit
    on ds.amount_unit_concept_id = amount_unit.concept_id
  left join {{ source('omop','concept') }} numer_unit
    on ds.numerator_unit_concept_id = numer_unit.concept_id
  left join {{ source('omop','concept') }} denom_unit
    on ds.denominator_unit_concept_id = denom_unit.concept_id
  where ds.drug_concept_id = {{ drug_concept_id }}
  limit 1
)
{% endmacro %}
