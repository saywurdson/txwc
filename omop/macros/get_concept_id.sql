{% macro lookup_concept_id(
    source_value,
    domain_id,
    standard_concept=None,
    invalid_reason=None,
    vocabulary_id=None,
    required_value=None
) %}
    {# Build the scalar subquery #}
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
          and c.vocabulary_id = '{{ vocabulary_id }}'
        {% endif %}
        limit 1
    )
    {% endset %}

    {# Cast the subquery result to integer, and use COALESCE if a fallback value is provided #}
    {% if required_value is not none %}
        coalesce(cast({{ subquery }} as integer), {{ required_value }})
    {% else %}
        cast({{ subquery }} as integer)
    {% endif %}
{% endmacro %}