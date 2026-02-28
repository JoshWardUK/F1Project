{{config (
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='circuitId'
)}}
SELECT *
FROM delta_scan('{{ get_delta_path("bronze", "circuits") }}') 