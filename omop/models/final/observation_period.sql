-- Observation period derived from the span of clinical events per person
-- For claims data, this represents the enrollment period where we have visibility into the patient's care
with clinical_events as (
    -- Combine all clinical event dates to find observation bounds
    select person_id, event_date
    from (
        -- Visit occurrences
        select person_id, visit_start_date as event_date
        from {{ ref('visit_occurrence') }}
        union all
        select person_id, visit_end_date as event_date
        from {{ ref('visit_occurrence') }}
        where visit_end_date is not null

        union all

        -- Condition occurrences
        select person_id, condition_start_date as event_date
        from {{ ref('condition_occurrence') }}
        union all
        select person_id, condition_end_date as event_date
        from {{ ref('condition_occurrence') }}
        where condition_end_date is not null

        union all

        -- Drug exposures
        select person_id, drug_exposure_start_date as event_date
        from {{ ref('drug_exposure') }}
        union all
        select person_id, drug_exposure_end_date as event_date
        from {{ ref('drug_exposure') }}
        where drug_exposure_end_date is not null

        union all

        -- Procedure occurrences
        select person_id, procedure_date as event_date
        from {{ ref('procedure_occurrence') }}

        union all

        -- Measurements
        select person_id, measurement_date as event_date
        from {{ ref('measurement') }}

        union all

        -- Observations
        select person_id, observation_date as event_date
        from {{ ref('observation') }}

        union all

        -- Device exposures
        select person_id, device_exposure_start_date as event_date
        from {{ ref('device_exposure') }}
    ) events
    where event_date is not null
),
person_observation_bounds as (
    select
        person_id,
        min(event_date) as observation_period_start_date,
        max(event_date) as observation_period_end_date
    from clinical_events
    group by person_id
)
select
    cast(row_number() over (order by person_id) as integer) as observation_period_id,
    cast(person_id as varchar) as person_id,
    cast(observation_period_start_date as date) as observation_period_start_date,
    cast(observation_period_end_date as date) as observation_period_end_date,
    cast(32855 as integer) as period_type_concept_id  -- Claim derived
from person_observation_bounds
where observation_period_start_date is not null
