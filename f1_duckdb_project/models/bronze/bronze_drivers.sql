SELECT *
FROM delta_scan('{{ get_delta_path("bronze", "drivers") }}')  