-- Maps the pre-renumbering visit_detail_id (hash-derived int from stg_visit_detail)
-- to the sequential 1..N id exposed by final.visit_detail.
-- visit_detail goes stg -> final directly (no int layer), so source is stg_visit_detail.
-- Stable across runs via explicit ORDER BY source_id.
select
    row_number() over (order by source_id) as visit_detail_id,
    source_id
from (
    select distinct cast(visit_detail_id as integer) as source_id
    from {{ ref('stg_visit_detail') }}
    where visit_detail_id is not null
)
