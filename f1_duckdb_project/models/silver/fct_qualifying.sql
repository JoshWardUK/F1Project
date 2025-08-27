{{ config(materialized='table', tags=['silver','fact']) }}

select
  try_cast(season as integer)            as season_year,
  try_cast(round as integer)             as round,
  nullif(trim(racename), '')             as race_name,

  circuitid                              as circuit_id,        -- keep as text
  nullif(trim(circuitname), '')          as circuit_name,

  nullif(trim(racedate), '')             as race_date_raw,

  driverid                               as driver_id,         -- keep as text
  nullif(trim(code), '')                 as driver_code,
  nullif(trim(givenname), '')            as given_name,
  nullif(trim(familyname), '')           as family_name,

  nullif(trim(constructor), '')          as constructor_name,
  constructorid                          as constructor_id,    -- keep as text

  try_cast(position as integer)          as quali_position,
  nullif(trim(q1), '')                   as q1_time_raw,
  nullif(trim(q2), '')                   as q2_time_raw,
  nullif(trim(q3), '')                   as q3_time_raw
from {{ ref('bronze_qualifying') }}