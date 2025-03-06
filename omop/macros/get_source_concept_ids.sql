{% macro get_source_concept_ids (
    source_value,
    domain_id,
    standard_concept=None,
    invalid_reason=None,
    vocabulary_id=None,
    required_value=None
) %}

    {% set subquery %}
    (
        select
            c.concept_id
        from {{ source('terminology', 'concept') }} as c
        where c.concept_code = {{ source_value | safe }}
          and c.domain_id = '{{ domain_id }}'
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
        {% if vocabulary_id is not none %}
          {% if vocabulary_id is string %}
            and c.vocabulary_id = '{{ vocabulary_id }}'
          {% else %}
            and c.vocabulary_id in (
              {% for vocab in vocabulary_id %}
                '{{ vocab }}'{% if not loop.last %}, {% endif %}
              {% endfor %}
            )
          {% endif %}
        {% endif %}
        limit 1
    )
    {% endset %}

    {% if required_value is not none %}
        coalesce(cast({{ subquery }} as integer), {{ required_value }})
    {% else %}
        cast({{ subquery }} as integer)
    {% endif %}
{% endmacro %}