{{ config(materialized='table') }}

-- Pre-resolved (source -> standard) concept mappings used by get_concept_ids.
-- Replaces per-row EXISTS+CASE+correlated-subquery with a small indexed lookup.
--
-- relationship_id semantics:
--   'identity'       -- concept is already a standard concept; source == target
--   'Maps to'        -- standard concept lookup via concept_relationship('Maps to')
--   'Maps to value'  -- observation-value lookup via concept_relationship('Maps to value')
--
-- All concept_id columns are emitted as INTEGER (omop.concept stores them as varchar
-- but every value is a valid integer per OMOP CDM). Filter on
-- (source_concept_id, source_domain_id, source_vocabulary_id, target_vocabulary_id, relationship_id)
-- depending on lookup intent.

with standard_concepts as (
    select
        cast(concept_id as integer) as concept_id,
        domain_id,
        vocabulary_id
    from {{ source('omop', 'concept') }}
    where standard_concept = 'S'
      and invalid_reason is null
),

identity_map as (
    select
        c.concept_id      as source_concept_id,
        c.domain_id       as source_domain_id,
        c.vocabulary_id   as source_vocabulary_id,
        c.concept_id      as target_concept_id,
        c.vocabulary_id   as target_vocabulary_id,
        cast('identity' as varchar) as relationship_id
    from standard_concepts c
),

relationship_map as (
    select
        cast(c1.concept_id as integer) as source_concept_id,
        c1.domain_id                   as source_domain_id,
        c1.vocabulary_id               as source_vocabulary_id,
        c2.concept_id                  as target_concept_id,
        c2.vocabulary_id               as target_vocabulary_id,
        cr.relationship_id             as relationship_id
    from {{ source('omop', 'concept_relationship') }} cr
    join {{ source('omop', 'concept') }} c1
      on cr.concept_id_1 = c1.concept_id
    join standard_concepts c2
      on cast(cr.concept_id_2 as integer) = c2.concept_id
    where cr.relationship_id in ('Maps to', 'Maps to value')
      and cr.invalid_reason is null
)

select * from identity_map
union all
select * from relationship_map
