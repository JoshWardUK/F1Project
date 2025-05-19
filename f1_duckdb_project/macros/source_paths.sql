{% macro get_delta_path(source_name, table_name) %}
    {% set base_path = '/Users/joshuaward/Documents/Data-Engineering/F1Project/F1Project/landing_zone/' %}
    {{ return(base_path ~ table_name) }}
{% endmacro %}