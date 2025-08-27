{{ config(materialized='table', tags=['silver','dim']) }}

select
        driverId                             as driver_id,
        nullif(trim(givenname), '')          as given_name,
        nullif(trim(familyname), '')         as family_name,
        try_cast(dateofbirth as date)        as date_of_birth,
        nullif(trim(drivernationality), '')  as nationality,

        constructorId                        as constructor_id,
        nullif(trim(constructorname), '')    as constructor_name,
        nullif(trim(constructornationality), '') as constructor_nationality,

        try_cast(season as integer)          as season_year,
        try_cast(round as integer)           as round,
        try_cast(position as integer)        as standing_position,
        nullif(trim(positiontext), '')       as standing_position_text,
        try_cast(points as double)           as points,
        try_cast(wins as integer)            as wins

from {{ ref('bronze_drivers') }}