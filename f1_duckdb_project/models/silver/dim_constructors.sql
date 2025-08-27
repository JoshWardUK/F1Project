{{ config(materialized='table', tags=['silver','dim']) }}

select
    constructorid                       as constructor_id,     -- keep as text
    nullif(trim(constructorname), '')   as constructor_name,
    nullif(trim(constructornationality), '') as constructor_nationality,
    try_cast(season as integer)         as season_year,
    try_cast(round as integer)          as round
from {{ ref('bronze_constructorstandings') }}
