{% set table_list = [
    ('institutional_header_current', 'raw'),
    ('institutional_header_historical', 'raw'),
    ('professional_header_current', 'raw'),
    ('professional_header_historical', 'raw'),
    ('pharmacy_header_current', 'raw'),
    ('pharmacy_header_historical', 'raw')
] %}

{% set cte_queries = [] %}

{% for table, schema in table_list %}
    {% if check_table_exists(schema, table) %}
        {% set cte_query %}
        {{ table }} as (
            select distinct
                cast(
                    hash(
                        concat_ws(
                            '||',
                            {% if 'pharmacy' in table %}
                                billing_provider_last_name,
                                billing_provider_fein
                            {% elif 'professional' in table %}
                                billing_provider_last_name,
                                facility_primary_address
                            {% else %}
                                billing_provider_last_name
                            {% endif %}
                        )
                    , 'xxhash64'
                    ) % 1000000000
                as varchar) as care_site_id,
                {% if 'pharmacy' in table %}
                    coalesce(facility_name, billing_provider_last_name) as care_site_name,
                    38004338 as place_of_service_concept_id,
                {% elif 'professional' in table %}
                    billing_provider_last_name as care_site_name,
                    8716 as place_of_service_concept_id,
                {% else %}
                    billing_provider_last_name as care_site_name,
                    8717 as place_of_service_concept_id,
                {% endif %}
                cast(
                    hash(
                        concat_ws(
                            '||',
                            {% if 'pharmacy' in table %}
                                billing_provider_last_name,
                                billing_provider_fein,
                                billing_provider_primary_1,
                                billing_provider_city,
                                billing_provider_state_code,
                                billing_provider_postal_code
                            {% else %}
                                billing_provider_last_name,
                                facility_primary_address,
                                facility_city,
                                facility_state_code,
                                facility_postal_code,
                                facility_country_code
                            {% endif %}
                        )
                    , 'xxhash64'
                    ) % 1000000000
                as varchar) as location_id,
                cast(null as varchar) as care_site_source_value,
                {% if 'institutional' in table %}
                cast(null as varchar) as place_of_service_source_value
                {% else %}
                cast(place_of_service_bill_code as varchar) as place_of_service_source_value
                {% endif %}
            from {{ source(schema, table) }}
        )
        {% endset %}
        {% do cte_queries.append(cte_query) %}
    {% endif %}
{% endfor %}

{% if cte_queries | length > 0 %}
with {{ cte_queries | join(', ') }}
{% endif %}

{% set valid_tables = [] %}
{% for table, schema in table_list %}
    {% if check_table_exists(schema, table) %}
         {% do valid_tables.append(table) %}
    {% endif %}
{% endfor %}

{% if valid_tables | length > 0 %}
select *
from (
    {% for table in valid_tables %}
        select * from {{ table }}
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}
) as final_result
{% else %}
select null as message
{% endif %}