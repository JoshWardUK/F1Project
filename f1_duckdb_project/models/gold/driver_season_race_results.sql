{{ config(materialized='table', tags=['gold','driver','races']) }}

with p as (select * from {{ ref('driver_season_params') }}),

base as (
  select
    r.season_year,
    r.round,
    r.race_name,
    r.circuit_id,
    r.circuit_name,
    fr.driver_id,
    fr.constructor_id,
    fr.grid_position,
    fr.finish_position,
    fr.points,
    fr.laps_completed,
    fr.status
  from {{ ref('fct_results') }} fr
  join {{ ref('dim_races') }}  r
    on r.season_year = fr.season_year and r.round = fr.round
  join p on p.season_year = fr.season_year
  where fr.driver_id = p.driver_id
),

enhanced as (
  select
    season_year,
    round,
    race_name,
    circuit_id,
    circuit_name,
    driver_id,
    constructor_id,
    grid_position,
    finish_position,
    points,
    laps_completed,
    status,
    case when grid_position is not null and finish_position is not null
         then grid_position - finish_position end as positions_gained,       -- +ve is good
    case when finish_position is not null and finish_position <= 3 then 1 else 0 end as is_podium,
    case when finish_position is null or lower(trim(status)) in ('dnf','dsq','did not finish','disqualified')
         then 1 else 0 end as is_dnf
  from base
)

select * from enhanced
order by round