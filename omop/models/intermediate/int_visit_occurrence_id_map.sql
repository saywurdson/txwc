-- Maps the pre-renumbering visit_occurrence_id (integer bill_id from int_visit_occurrence)
-- to the sequential 1..N id exposed by final.visit_occurrence.
-- Stable across runs via explicit ORDER BY source_id.
select
    row_number() over (order by source_id) as visit_occurrence_id,
    source_id
from (
    select distinct cast(visit_occurrence_id as integer) as source_id
    from {{ ref('int_visit_occurrence') }}
    where visit_occurrence_id is not null
)
