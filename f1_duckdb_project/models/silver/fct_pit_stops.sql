{{ config(materialized='table', tags=['silver','fact']) }}

select
  try_cast(season as integer)            as season_year,
  try_cast(round as integer)             as round,
  nullif(trim(racename), '')             as race_name,

  circuitid                              as circuit_id,        -- keep as text
  nullif(trim(circuitname), '')          as circuit_name,

  nullif(trim(date), '')                 as race_date_raw,
  nullif(trim(time), '')                 as race_time_raw,

  driverid                               as driver_id,         -- keep as text
  try_cast(lap as integer)               as lap_number,
  try_cast(stop as integer)              as stop_number,
  nullif(trim(pittime), '')              as pit_time_of_day_raw,
  nullif(trim(duration), '')             as stop_duration_raw
from {{ ref('bronze_pitstops') }}