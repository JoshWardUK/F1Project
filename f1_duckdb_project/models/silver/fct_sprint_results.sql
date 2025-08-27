{{ config(materialized='table', tags=['silver','fact']) }}

select
  try_cast(season as integer)            as season_year,
  try_cast(round as integer)             as round,
  nullif(trim(racename), '')             as race_name,

  circuitid                              as circuit_id,        -- keep as text
  nullif(trim(circuitname), '')          as circuit_name,

  nullif(trim(racedate), '')             as race_date_raw,
  nullif(trim(racetime), '')             as race_time_raw,

  driverid                               as driver_id,         -- keep as text
  nullif(trim(code), '')                 as driver_code,
  nullif(trim(givenname), '')            as given_name,
  nullif(trim(familyname), '')           as family_name,

  nullif(trim(constructor), '')          as constructor_name,
  constructorid                          as constructor_id,    -- keep as text

  try_cast(grid as integer)              as grid_position,
  try_cast(position as integer)          as finish_position,
  nullif(trim(positiontext), '')         as finish_position_text,
  try_cast(points as double)             as points,
  try_cast(laps as integer)              as laps_completed,
  nullif(trim(status), '')               as status,

  nullif(trim(sprinttime), '')           as sprint_time_raw,
  try_cast(sprinttimemillis as bigint)   as sprint_time_ms,

  try_cast(fastestlap as integer)        as fastest_lap_number,
  nullif(trim(fastestlaptime), '')       as fastest_lap_time_raw
from {{ ref('bronze_sprint') }}