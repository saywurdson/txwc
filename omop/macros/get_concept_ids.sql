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
{#-
  Resolve a source concept_id to a standard concept_id.

  Two-step lookup against the pre-built int_concept_map (replaces the previous
  per-row EXISTS+CASE+correlated-subquery pattern):

    1. Identity:  the source concept is already standard in the requested
                  domain + vocabulary -> return it as-is.
    2. Mapping:   the source maps to a standard concept via concept_relationship
                  ('Maps to' or 'Maps to value') with target_vocabulary matching.
    3. Fallback:  required_value (default 0) when neither lookup hits.

  Determinism: when a source concept has multiple valid mappings in the target
  vocabulary (e.g., one ICD code -> multiple SNOMED concepts), we order by
  target_concept_id ASC and pick the lowest. The previous per-row macro used
  LIMIT 1 with no ORDER BY, so its winner depended on warehouse scan order.
  This is deterministic and stable across re-runs.

  standard_concept and invalid_reason are baked into int_concept_map (always 'S'
  and NULL respectively) and the parameters are kept for backward compatibility
  with existing call sites; non-default values would not be honored, so a
  compile error is raised in that case.
-#}
{%- if standard_concept != 'S' -%}
  {{ exceptions.raise_compiler_error("get_concept_ids: standard_concept must be 'S' (baked into int_concept_map)") }}
{%- endif -%}
{%- if invalid_reason | lower != 'is null' -%}
  {{ exceptions.raise_compiler_error("get_concept_ids: invalid_reason must be 'is null' (baked into int_concept_map)") }}
{%- endif -%}
(
  select coalesce(
    -- Identity: source is already standard in the requested domain + vocabulary
    (
      select target_concept_id
      from {{ ref('int_concept_map') }}
      where relationship_id = 'identity'
        and source_concept_id = {{ source_concept_id | safe }}
        {%- if domain_id is string %}
        and source_domain_id = '{{ domain_id }}'
        {%- else %}
        and source_domain_id in (
          {%- for d in domain_id %}'{{ d }}'{% if not loop.last %}, {% endif %}{% endfor -%}
        )
        {%- endif %}
        and source_vocabulary_id in (
          {%- for vocab in vocabulary_id %}'{{ vocab }}'{% if not loop.last %}, {% endif %}{% endfor -%}
        )
      order by target_concept_id
      limit 1
    ),
    -- Cross-reference via concept_relationship
    (
      select target_concept_id
      from {{ ref('int_concept_map') }}
      where relationship_id = '{{ relationship_id }}'
        and source_concept_id = {{ source_concept_id | safe }}
        and source_vocabulary_id in (
          {%- for vocab in vocabulary_id %}'{{ vocab }}'{% if not loop.last %}, {% endif %}{% endfor -%}
        )
        {%- if vocabulary_target is string %}
        and target_vocabulary_id = '{{ vocabulary_target }}'
        {%- else %}
        and target_vocabulary_id in (
          {%- for vocab in vocabulary_target %}'{{ vocab }}'{% if not loop.last %}, {% endif %}{% endfor -%}
        )
        {%- endif %}
      order by target_concept_id
      limit 1
    ),
    {% if required_value is not none %}{{ required_value }}{% else %}0{% endif %}
  )
)
{% endmacro %}
