-- macros/tests/is_valid_year.sql
{% test is_valid_year(model, column_name) %}
SELECT *
FROM {{ model }}
WHERE
    -- not castable to integer
    TRY_CAST({{ column_name }} AS INTEGER) IS NULL
    -- or out of acceptable range
    OR CAST({{ column_name }} AS INTEGER) < 1900
    OR CAST({{ column_name }} AS INTEGER) > 2100
{% endtest %}