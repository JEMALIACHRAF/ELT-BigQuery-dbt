-- macros/assert_no_future_dates.sql
-- Custom test: ensure date column has no future values

{% test assert_no_future_dates(model, column_name) %}

select
    {{ column_name }},
    count(*) as nb_rows
from {{ model }}
where {{ column_name }} > current_date()
group by 1
having count(*) > 0

{% endtest %}


-- macros/generate_schema_name.sql
-- Override default schema naming to support multi-env deployments

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}

    {%- if target.name == 'prod' -%}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}
    {%- else -%}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ default_schema }}_{{ custom_schema_name | trim }}
        {%- endif -%}
    {%- endif -%}

{%- endmacro %}


-- macros/cents_to_euros.sql
-- Utility macro: convert cents to euros

{% macro cents_to_euros(column_name) %}
    round(cast({{ column_name }} as numeric) / 100.0, 2)
{% endmacro %}
