-- Normalize care_site names and deduplicate by care_site_id
with normalized_care_sites as (
    select
        care_site_id,
        -- Normalize common variations to canonical names
        case
            -- BIR JV variations (Baylor Institute for Rehabilitation Joint Venture)
            when regexp_matches(care_site_name, '^BIR\s*JV') then 'BIR JV LLP'

            -- BTDI JV variations (Baylor Therapy Development Inc Joint Venture) - catch all typos
            when regexp_matches(care_site_name, '^B\s*T\s*D\s*I\s*JV') then 'BTDI JV LLP'
            when regexp_matches(care_site_name, '^BTD\s*JV') then 'BTDI JV LLP'
            when regexp_matches(care_site_name, '^BTDJ\s*JV') then 'BTDI JV LLP'
            when regexp_matches(care_site_name, '^BTOI\s*JV') then 'BTDI JV LLP'
            when regexp_matches(care_site_name, '^STDI\s*JV') then 'BTDI JV LLP'
            when regexp_matches(care_site_name, '^BIDI\s*JV') then 'BTDI JV LLP'
            when regexp_matches(care_site_name, '^BRDI\s*JV') then 'BTDI JV LLP'

            -- BAMC MCHE variations - normalize to canonical
            when regexp_matches(care_site_name, '^BAMC MCHE COU.?M\s*DEPT\s*211$') then 'BAMC MCHE COU M DEPT 211'
            when regexp_matches(care_site_name, '^BAMC MCHE COU.?M\s*DEPT\s*201$') then 'BAMC MCHE COU M DEPT 201'
            when regexp_matches(care_site_name, '^BAMC MCHE COU.?M$') then 'BAMC MCHE COU M'
            when regexp_matches(care_site_name, '^BAMC MCHE PAA.?M?\s*DEPT') then 'BAMC MCHE PAA DEPT'

            -- HARRIS COUNTY ESD variations - normalize removing 'NO' and '#'
            when regexp_matches(care_site_name, '^HARRIS COUNTY ESD\s*(NO\s*|#)?\d+$') then
                'HARRIS COUNTY ESD ' || regexp_extract(care_site_name, '\d+$')

            -- ADVANCED RX variations - normalize to single format
            when regexp_matches(care_site_name, '^ADVANCED RX\s*(LLC|PHARMACY)?\s*\d+$') then
                'ADVANCED RX ' || regexp_extract(care_site_name, '\d+$')

            -- MEDSPRING variations - all consolidate to one
            when regexp_matches(care_site_name, '^MEDSPRING') then 'MEDSPRING OF TEXAS PA'

            -- BLUE STONE JV
            when regexp_matches(care_site_name, '^BLUE\s*STONE\s*JV') then 'BLUE STONE JV LLP'

            -- PRO MED REHAB variations
            when regexp_matches(care_site_name, '^(MEC ASC.?)?PRO MED') then 'PRO MED REHAB'

            -- Strip trailing numbers from generic patterns (e.g., "REHAB 2112")
            when regexp_matches(care_site_name, '^REHAB\s+\d+$') then 'REHAB CENTER'

            -- US TOXICOLOGY variations
            when regexp_matches(care_site_name, '^US TOXICOLOGY') then 'US TOXICOLOGY LLC'

            -- Keep original name for all others
            else care_site_name
        end as care_site_name,
        place_of_service_concept_id,
        location_id,
        care_site_source_value,
        place_of_service_source_value,
        -- Rank to pick best record per care_site_id
        row_number() over (
            partition by care_site_id
            order by
                -- Prefer records with longer care_site_name (more descriptive)
                length(coalesce(care_site_name, '')) desc,
                -- Then prefer records with location_id
                case when location_id is not null then 0 else 1 end
        ) as rn
    from {{ ref('stg_care_site') }}
)
select distinct
    cast(care_site_id as integer) as care_site_id,
    cast(care_site_name as varchar) as care_site_name,
    cast(place_of_service_concept_id as varchar) as place_of_service_concept_id,
    cast(location_id as integer) as location_id,
    cast(care_site_source_value as varchar) as care_site_source_value,
    cast(place_of_service_source_value as varchar) as place_of_service_source_value
from normalized_care_sites
where rn = 1