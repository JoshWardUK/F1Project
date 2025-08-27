{{ config(materialized='table', tags=['silver','dim']) }}

select
  circuitid                              as circuit_id,         -- keep as text
  nullif(trim(circuitname), '')          as circuit_name,
  nullif(trim(url), '')                  as url,
  nullif(trim(locality), '')             as city,
  nullif(trim(country), '')              as country,
  try_cast(latitude as double)           as latitude,
  try_cast(longitude as double)          as longitude
from {{ ref('bronze_circuits') }}