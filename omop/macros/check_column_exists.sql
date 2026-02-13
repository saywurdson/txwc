{% macro check_column_exists(schema_name, table_name, column_name) %}
    {% set query %}
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = '{{ schema_name }}'
            AND table_name = '{{ table_name }}'
            AND column_name = '{{ column_name }}'
        );
    {% endset %}

    {% set results = run_query(query) %}
    {% if execute %}
        {% set column_exists = results.columns[0][0] %}
    {% else %}
        {% set column_exists = false %}
    {% endif %}

    {{ return(column_exists) }}
{% endmacro %}
