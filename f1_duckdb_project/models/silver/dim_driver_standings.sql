{{ config(materialized='table', tags=['silver','dim']) }}

select
  try_cast(season as integer)            as season_year,
  try_cast(round as integer)             as round,
  try_cast(position as integer)          as standing_position,
  nullif(trim(positiontext), '')         as standing_position_text,
  try_cast(points as double)             as points,
  try_cast(wins as integer)              as wins,

  driverid                               as driver_id,          -- keep as text
  nullif(trim(drivername), '')           as driver_name,
  nullif(trim(drivernationality), '')    as driver_nationality,
  try_cast(driverdob as date)            as driver_dob,
  nullif(trim(drivercode), '')           as driver_code,
  nullif(trim(driverurl), '')            as driver_url,

  constructorid                          as constructor_id,     -- keep as text
  nullif(trim(constructorname), '')      as constructor_name,
  nullif(trim(constructornationality), '') as constructor_nationality,
  nullif(trim(constructorurl), '')       as constructor_url
from {{ ref('bronze_driver_standings') }}