{{ config(materialized='table', tags=['silver','dim']) }}

select
  try_cast(season as integer)            as season_year,
  try_cast(round as integer)             as round,
  try_cast(position as integer)          as standing_position,
  nullif(trim(positiontext), '')         as position_text,
  try_cast(points as double)             as points,
  try_cast(wins as integer)              as wins,

  constructorid                          as constructor_id,    -- keep as text
  nullif(trim(constructorname), '')      as constructor_name,
  nullif(trim(constructornationality), '') as constructor_nationality
from {{ ref('bronze_constructorstandings') }}