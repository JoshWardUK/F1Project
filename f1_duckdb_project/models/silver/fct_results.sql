{{ config(materialized='table', tags=['silver','fact']) }}

select
  try_cast(season as integer)            as season_year,
  try_cast(round as integer)             as round,
  nullif(trim(racename), '')             as race_name,

  circuitid                              as circuit_id,         -- keep as text
  nullif(trim(circuitname), '')          as circuit_name,

  nullif(trim(date), '')                 as race_date_raw,
  nullif(trim(time), '')                 as time_raw,           -- parser may overwrite 'time' with result time

  driverid                                as driver_id,         -- keep as text
  nullif(trim(drivername), '')            as driver_name,

  constructorid                           as constructor_id,    -- keep as text
  nullif(trim(constructorname), '')       as constructor_name,

  try_cast(grid as integer)               as grid_position,
  try_cast(position as integer)           as finish_position,
  try_cast(points as double)              as points,
  try_cast(laps as integer)               as laps_completed,
  nullif(trim(status), '')                as status,
  nullif(trim(time), '')                  as result_time_raw,   -- keep string time from results

  try_cast(fastestlap as integer)         as fastest_lap_number,
  nullif(trim(fastestlaptime), '')        as fastest_lap_time_raw,
  nullif(trim(averagespeed), '')          as average_speed_raw
from {{ ref('bronze_results') }}