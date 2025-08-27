{{ config(materialized='table', tags=['silver','dim']) }}

select
    try_cast(season as integer)          as season_year,
    try_cast(round as integer)           as round,
    nullif(trim(racename), '')           as race_name,

    circuitid                            as circuit_id,         -- keep as text
    nullif(trim(circuitname), '')        as circuit_name,
    try_cast(lat as double)              as latitude,
    try_cast(long as double)             as longitude,
    nullif(trim(locality), '')           as city,
    nullif(trim(country), '')            as country,

    nullif(trim(racedate), '')           as race_date_raw,
    nullif(trim(racetime), '')           as race_time_raw,
    nullif(trim(fp1date), '')            as fp1_date_raw,
    nullif(trim(fp1time), '')            as fp1_time_raw,
    nullif(trim(fp2date), '')            as fp2_date_raw,
    nullif(trim(fp2time), '')            as fp2_time_raw,
    nullif(trim(fp3date), '')            as fp3_date_raw,
    nullif(trim(fp3time), '')            as fp3_time_raw,
    nullif(trim(qualifyingdate), '')     as qualifying_date_raw,
    nullif(trim(qualifyingtime), '')     as qualifying_time_raw,
    nullif(trim(sprintdate), '')         as sprint_date_raw,
    nullif(trim(sprinttime), '')         as sprint_time_raw
from {{ ref('bronze_races') }}