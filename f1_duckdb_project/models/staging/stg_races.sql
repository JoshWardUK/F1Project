-- models/staging/stg_races.sql
SELECT
  circuitId,
  racename,
  racedate
FROM delta_scan('/Users/joshuaward/Documents/Data-Engineering/F1Project/F1Project/landing_zone/races/')
