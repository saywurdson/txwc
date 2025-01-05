{% macro check_table_exists(schema_name, table_name) %}
    {% set query %}
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = '{{ schema_name }}' 
            AND table_name = '{{ table_name }}'
        );
    {% endset %}

    {% set results = run_query(query) %}
    {% if execute %}
        {% set table_exists = results.columns[0][0] %}
    {% else %}
        {% set table_exists = false %}
    {% endif %}

    {{ return(table_exists) }}
{% endmacro %}