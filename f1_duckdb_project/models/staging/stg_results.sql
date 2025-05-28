--models/staging/stg_results.sql

SELECT
  
  season::varchar as season,
  round::int as round,
  raceName::varchar as raceName,
  circuitId::varchar as circuitId,
  circuitName::varchar as circuitName,
  date::date as date,
  time::varchar as time,
  driverId::varchar as driverId,
  driverName::varchar as driverName,
  constructorId::varchar as constructorId,
  constructorName::varchar as constructorName,
  grid::int as grid,
  position::int as position,
  points::int as points,
  laps::int as laps,
  status::varchar as status,
  fastestLap::varchar as fastestLap,
  fastestLapTime::varchar as fastestLapTime,
  try_cast(averageSpeed as decimal) as averageSpeed

FROM delta_scan('{{ get_delta_path("raw", "results") }}')  