{{ config(materialized='table', tags=['silver','fact']) }}

select
  try_cast(season as integer)            as season_year,
  try_cast(round as integer)             as round,
  nullif(trim(racename), '')             as race_name,

  circuitid                              as circuit_id,        -- keep as text
  nullif(trim(circuitname), '')          as circuit_name,

  nullif(trim(racedate), '')             as race_date_raw,

  try_cast(lapnumber as integer)         as lap_number,
  driverid                               as driver_id,         -- keep as text
  try_cast(position as integer)          as position_on_lap,
  nullif(trim(laptime), '')              as lap_time_raw
from {{ ref('bronze_laps') }}