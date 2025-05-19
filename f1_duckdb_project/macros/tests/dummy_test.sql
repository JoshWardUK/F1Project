{% test dummy_test(model, column_name) %}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} = 'Schumacher'
{% endtest %}