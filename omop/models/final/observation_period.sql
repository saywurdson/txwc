-- Observation period derived from the span of clinical events per person.
-- For workers' comp claims, the lower bound is the employee_date_of_injury
-- (surfaced via observation_concept_id = 40771952 'Injury date') when present,
-- falling back to the earliest clinical event otherwise. The upper bound is
-- always the latest clinical event.
with injury_dates as (
    select
        person_id,
        min(observation_date) as earliest_injury_date
    from {{ ref('observation') }}
    where observation_concept_id = 40771952  -- LOINC 'Injury date'
      and observation_date is not null
    group by person_id
),
clinical_events as (
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
person_clinical_bounds as (
    select
        person_id,
        min(event_date) as earliest_clinical_event,
        max(event_date) as observation_period_end_date
    from clinical_events
    group by person_id
),
person_observation_bounds as (
    -- Start = earliest of (injury date, earliest clinical event) so every clinical
    -- event is guaranteed to fall within the observation_period (OMOP CDM requires
    -- all events to be covered). For clean workers' comp data, injury_date is usually
    -- earlier than all care, and LEAST picks it. If any clinical event predates the
    -- injury date (data anomaly), LEAST picks that earlier event so we stay compliant.
    -- DuckDB's LEAST ignores NULL args, so a missing injury_date falls back correctly.
    select
        c.person_id,
        least(i.earliest_injury_date, c.earliest_clinical_event) as observation_period_start_date,
        c.observation_period_end_date
    from person_clinical_bounds c
    left join injury_dates i on c.person_id = i.person_id
)
select
    cast(row_number() over (order by person_id) as integer) as observation_period_id,
    cast(person_id as integer) as person_id,
    cast(observation_period_start_date as date) as observation_period_start_date,
    cast(observation_period_end_date as date) as observation_period_end_date,
    cast(32855 as integer) as period_type_concept_id  -- Claim derived
from person_observation_bounds
where observation_period_start_date is not null
