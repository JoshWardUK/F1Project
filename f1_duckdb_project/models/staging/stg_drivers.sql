-- models/staging/stg_races.sql
SELECT

  season::VARCHAR AS season,
  round::INT AS round,
  position::INT AS position,
  positionText::VARCHAR AS positionText,
  points::INT AS points,
  wins::INT AS wins,
  driverId::VARCHAR AS driverId,
  givenName::VARCHAR AS givenName,
  familyName::VARCHAR AS familyName,
  dateOfBirth::DATE AS dateOfBirth,
  driverNationality::VARCHAR AS driverNationality,
  constructorId::VARCHAR AS constructorId,
  constructorName::VARCHAR AS constructorName,
  constructorNationality::VARCHAR AS constructorNationality

FROM delta_scan('{{ get_delta_path("raw", "drivers") }}')