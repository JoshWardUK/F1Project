{% macro duckdb__drop_relation(relation) %}
  {# relation.type is usually 'table' or 'view' #}
  {% if relation is none %}
    {{ return('') }}
  {% endif %}

  {% if relation.type == 'view' %}
    drop view if exists {{ relation }}
  {% else %}
    drop table if exists {{ relation }}
  {% endif %}
{% endmacro %}