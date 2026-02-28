{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'circuit_id',
    tags=['silver','dim']
) }}

WITH ranked AS (
  SELECT
    circuitid                              as circuit_id,         -- keep as text
    nullif(trim(circuitname), '')          as circuit_name,
    nullif(trim(url), '')                  as url,
    nullif(trim(locality), '')             as city,
    nullif(trim(country), '')              as country,
    try_cast(latitude as double)           as latitude,
    try_cast(longitude as double)          as longitude,
    ROW_NUMBER() OVER (
              PARTITION BY circuitid
              ORDER BY
                  circuitname,
                  country,
                  locality
          ) AS rn
  FROM {{ ref('bronze_circuits') }}

)
SELECT *
FROM ranked
WHERE rn = 1