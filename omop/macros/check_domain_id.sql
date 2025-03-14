{% macro check_domain_id(source_table, source_columns, target_domain, vocabulary_ids) %}
exists (
    select 1
    from {{ source('omop', 'concept') }} as c
    where (
        {% for column in source_columns %}
            c.concept_code = {{ source_table }}.{{ column }}
            {% if not loop.last %} or {% endif %}
        {% endfor %}
    )
    and c.domain_id = '{{ target_domain }}'
    and c.vocabulary_id in (
        {% for vocab in vocabulary_ids %}
            '{{ vocab }}'{% if not loop.last %}, {% endif %}
        {% endfor %}
    )
)
{% endmacro %}