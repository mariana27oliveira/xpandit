-- Takes a number of SQL queries separated by a ';' and runs them individually.

{% macro run_queries(queries) -%}
  {% for query in (queries | trim | trim(';')).split(';') -%}
    {%- do run_query(query | trim) -%}
  {%- endfor %}
{%- endmacro %}