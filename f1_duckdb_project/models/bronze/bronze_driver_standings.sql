{{config (
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='driverId,season,round'
)}}
SELECT *
FROM delta_scan('{{ get_delta_path("bronze", "driverstandings") }}')  