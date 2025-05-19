SELECT
  driverId,
  givenname,
  familyname,
  drivernationality
FROM delta_scan('{{ get_delta_path("raw", "drivers") }}')