{% macro get_concept_ids(
    source_value,
    domain_id,
    vocabulary_id,
    to,
    standard_concept='S',
    invalid_reason='is null'
) %}
(
    select
        case
            when src.concept_id is not null then cast(src.concept_id as integer)
            else coalesce(
                cast(
                    (
                        select c2.concept_id
                        from {{ source('terminology','concept_relationship') }} as cr
                        join {{ source('terminology','concept') }} as c1
                            on cr.concept_id_1 = c1.concept_id
                        join {{ source('terminology','concept') }} as c2
                            on cr.concept_id_2 = c2.concept_id
                        where cr.relationship_id ilike 'Maps to'
                            and c1.concept_code = {{ source_value | safe }}
                            and c1.vocabulary_id in (
                                {% for vocab in vocabulary_id %}
                                '{{ vocab }}'{% if not loop.last %}, {% endif %}
                                {% endfor %}
                            )
                            and c2.vocabulary_id = '{{ to }}'
                            and c1.invalid_reason is null
                            and c2.invalid_reason is null
                            and c2.standard_concept = '{{ standard_concept }}'
                        limit 1
                    ) as integer
                ),
                0
            )
        end as condition_concept_id
    from (
        select c.concept_id
        from {{ source('terminology','concept') }} as c
        where c.concept_code = {{ source_value | safe }}
            and c.domain_id = '{{ domain_id }}'
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
    ) as src
)
{% endmacro %}