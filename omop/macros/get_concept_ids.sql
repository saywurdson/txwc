{% macro get_concept_ids(
    source_concept_id,
    domain_id,
    vocabulary_id,
    vocabulary_target,
    relationship_id='Maps to',
    standard_concept='S',
    invalid_reason='is null',
    required_value=None
) %}
(
  select coalesce(derived.result, {% if required_value is not none %}{{ required_value }}{% else %}0{% endif %}) as condition_concept_id
  from (
    select case
      when exists(
        select 1
        from {{ source('terminology','concept') }} as c
        where c.concept_id = {{ source_concept_id | safe }} 
          and 
          {% if domain_id is string %}
            c.domain_id = '{{ domain_id }}'
          {% else %}
            c.domain_id in (
              {% for d in domain_id %}
                '{{ d }}'{% if not loop.last %}, {% endif %}
              {% endfor %}
            )
          {% endif %}
          and c.vocabulary_id in (
            {% for vocab in vocabulary_id %}
              '{{ vocab }}'{% if not loop.last %}, {% endif %}
            {% endfor %}
          )
          {% if standard_concept is not none %}
            and c.standard_concept = '{{ standard_concept }}'
          {% endif %}
          {% if invalid_reason is not none %}
            {% if invalid_reason | lower == 'is null' %}
              and c.invalid_reason is null
            {% else %}
              and c.invalid_reason = '{{ invalid_reason }}'
            {% endif %}
          {% endif %}
      )
      then cast(
          (
            select c.concept_id
            from {{ source('terminology','concept') }} as c
            where c.concept_id = {{ source_concept_id | safe }} 
              and 
              {% if domain_id is string %}
                c.domain_id = '{{ domain_id }}'
              {% else %}
                c.domain_id in (
                  {% for d in domain_id %}
                    '{{ d }}'{% if not loop.last %}, {% endif %}
                  {% endfor %}
                )
              {% endif %}
              and c.vocabulary_id in (
                {% for vocab in vocabulary_id %}
                  '{{ vocab }}'{% if not loop.last %}, {% endif %}
                {% endfor %}
              )
              {% if standard_concept is not none %}
                and c.standard_concept = '{{ standard_concept }}'
              {% endif %}
              {% if invalid_reason is not none %}
                {% if invalid_reason | lower == 'is null' %}
                  and c.invalid_reason is null
                {% else %}
                  and c.invalid_reason = '{{ invalid_reason }}'
                {% endif %}
              {% endif %}
            limit 1
          ) as integer
      )
      else cast(
          (
            select c2.concept_id
            from {{ source('terminology','concept_relationship') }} as cr
            join {{ source('terminology','concept') }} as c1
              on cr.concept_id_1 = c1.concept_id
            join {{ source('terminology','concept') }} as c2
              on cr.concept_id_2 = c2.concept_id
            where cr.relationship_id ilike '{{ relationship_id }}'
              and c1.concept_id = {{ source_concept_id | safe }} 
              and c1.vocabulary_id in (
                {% for vocab in vocabulary_id %}
                  '{{ vocab }}'{% if not loop.last %}, {% endif %}
                {% endfor %}
              )
              {% if vocabulary_target is string %}
                and c2.vocabulary_id = '{{ vocabulary_target }}'
              {% else %}
                and c2.vocabulary_id in (
                  {% for vocab in vocabulary_target %}
                    '{{ vocab }}'{% if not loop.last %}, {% endif %}
                  {% endfor %}
                )
              {% endif %}
              and c1.invalid_reason is null
              and c2.invalid_reason is null
              and c2.standard_concept = '{{ standard_concept }}'
            limit 1
          ) as integer
      )
    end as result
    from (values (1)) as dummy(dummy_val)
  ) as derived
)
{% endmacro %}