-- models/staging/stg_races.sql
SELECT
raceName,driverId,points
FROM delta_scan('/Users/joshuaward/Documents/Data-Engineering/F1Project/F1Project/landing_zone/results//////')


