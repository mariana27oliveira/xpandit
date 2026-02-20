--SCD1??

{% macro gold_merge(src_table=none, primary_keys=none) -%}

{% if execute %}
    {% if src_table is none %}
        {% do exceptions.raise_compiler_error('Error: src_table must be defined!') %}
    {% endif %}
    {% if primary_keys is none %}
        {% do exceptions.raise_compiler_error('Error: primary_keys must be defined!') %}
    {% endif %}
{% endif %}

{% set table_columns = adapter.get_columns_in_relation(src_table) %}
{% set filtered_columns = [] %}
{% for col in table_columns %}
    {% if col.name.lower() != "meta_is_last_move" %}
        {% do filtered_columns.append(col) %}
    {% endif %}
{% endfor %}
{% set table_columns = filtered_columns %}

MERGE INTO {{ this }} as target 
USING 
(

    SELECT *
    FROM {{ src_table }}
    WHERE meta_is_last_move = 'Y'

) 
AS source ON 
    {% for pk in primary_keys %}target.{{ pk }} = source.{{ pk }} {% if not loop.last %}AND{% endif %} 
    {% endfor %}

WHEN MATCHED AND target.meta_row_hash <> source.meta_row_hash THEN
    UPDATE SET 
        {% for col in table_columns %}target.{{ col.name }} = source.{{ col.name }},
        {% endfor %}target.meta_updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
    INSERT
        (
            {% for col in table_columns %}{{ col.name }},
            {% endfor %}meta_created_at,
            meta_updated_at
        )
    VALUES
        (
            {% for col in table_columns %}source.{{ col.name }},
            {% endfor %}CURRENT_TIMESTAMP(),
            null
        )
        
{%- endmacro %}