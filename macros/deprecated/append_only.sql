{% macro append_only(
    src_table=none, 
    surrogate_key=none,
    time_column=none,
    exec_date=none,
    aliases=none,
    exclusion_columns=none
)-%}

{% if execute %}
    {% if config.get('materialized') != 'incremental' %}
        {% do exceptions.raise_compiler_error('Error: This template must be used with an incremental materialization') %}
    {% endif %}
    {% if src_table is none %}
        {% do exceptions.raise_compiler_error('Error: src_table must be defined!') %}
    {% endif %}
    {% if time_column is none %}
        {% do exceptions.raise_compiler_error('Error: time_column must be defined!') %}
    {% endif %}

    {% if is_incremental() and exec_date is none %}
        {% do exceptions.raise_compiler_error('Error: exec_date must be defined for incremental executions') %}
    {% endif %}
{% endif %}

{% set table_columns = adapter.get_columns_in_relation(src_table) %}

{# -- IF SURROGATE_KEY IS REQUIRED
 #}
{% set has_suk = surrogate_key is not none %}

{%- if exclusion_columns is not none -%}
    {%- set exclusion_set = exclusion_columns if exclusion_columns is iterable and exclusion_columns is not string else [exclusion_columns] -%}
    {%- set filtered_columns = [] -%}
    {%- for col in table_columns -%}
        {%- if col.name not in exclusion_set -%}
            {%- do filtered_columns.append(col) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- set table_columns = filtered_columns -%}
{%- endif -%}

{% set add_meta_exec_and_creation_dates = true %}

WITH incoming_update AS (
    SELECT
        {% for col in table_columns %}
            {% if aliases is not none and col.name in aliases %}
                {{ col.name }} AS {{aliases.get(col.name)}}
            {%- else -%}
                {{ col.name }}
            {% endif %}
            {% if not loop.last or add_meta_exec_and_creation_dates %},{% endif %} 
        {% endfor %}
        CURRENT_TIMESTAMP() AS meta_exec_date,
        1 AS meta_is_current
    FROM {{ src_table }}
    {% if is_incremental() or exec_date is not none %}WHERE {{time_column}} = '{{exec_date}}'{% endif %}
)

SELECT
    {# -- CALCULATES SURROGATE_KEY IF REQUIRED
    #}
    {% if has_suk %}
        xxhash64(
        {%- for col in table_columns -%}
            {%- if aliases is not none and col.name in aliases -%}
            {{ aliases.get(col.name) }}
            {%- else -%}
            {{ col.name }}
            {%- endif -%}
            {%- if not loop.last -%},{%- endif -%}
        {%- endfor -%}
        , meta_exec_date
        , meta_is_current
        ) AS {{ surrogate_key }},
    {% endif %}
    incoming_update.*
FROM incoming_update

{%- endmacro %}
