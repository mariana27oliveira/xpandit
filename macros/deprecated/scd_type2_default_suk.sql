{% macro scd_type2_default_suk(
    src_table=none, 
    surrogate_key=none,
    time_column=none,
    exec_date=none,
    aliases=none,
    column_rename=none,
    exclusion_columns=none,
    add_meta_exec_date=True,
    truncate_meta_date=True
) -%}

{% if execute %}
    {% if src_table is none %}
        {% do exceptions.raise_compiler_error('Error: src_table must be defined!') %}
    {% endif %}
    {% if surrogate_key is none %}
        {% do exceptions.raise_compiler_error('Error: surrogate_key must be defined!') %}
    {% endif %}
    {% if time_column is none %}
        {% do exceptions.raise_compiler_error('Error: time_column must be defined!') %}
    {% endif %}
    {% if exec_date is none %}
        {% do exceptions.raise_compiler_error('Error: exec_date must be defined!') %}
    {% endif %}
{% endif %}

{% set table_columns = adapter.get_columns_in_relation(src_table) %}

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

{% set table_columns_no_metadata = [] %}
{% for col in table_columns %}
    {% if not col.name.startswith("meta") %}
        {% do table_columns_no_metadata.append(col) %}
    {% endif %}
{% endfor %}


WITH incoming_update AS (

    SELECT
        xxhash64(
            {% for col in table_columns_no_metadata %}{{ col.name }}{% if not loop.last %},{% endif %}
            {% endfor %}) AS {{ surrogate_key }},
        {% for col in table_columns %}{%- if column_rename is not none and col.name in column_rename -%}{{ column_rename.get(col.name) }}{% elif aliases is not none and col.name in aliases %}{{ col.name }} AS {{aliases.get(col.name)}}{%- else -%}{{ col.name }}{% endif %}, 
        {% endfor %}{% if add_meta_exec_date %}CURRENT_TIMESTAMP() AS meta_exec_date,
        {% endif %}
        
        {% if truncate_meta_date %}
            TO_TIMESTAMP(TO_DATE({{ time_column }})) AS meta_valid_from,
        {% else %}
            TO_TIMESTAMP({{ time_column }}) AS meta_valid_from,
        {% endif %}

        TO_TIMESTAMP('31/12/9999 00:00:00', 'dd/MM/yyyy HH:mm:ss') AS meta_valid_to,
        1 AS meta_is_current
    FROM {{ src_table }}
    WHERE TO_DATE({{time_column}}) = TO_DATE('{{exec_date}}')

)


MERGE INTO {{ this }} as target
USING (

    SELECT 
        incoming_update.{{ surrogate_key }} as mergeKey1,
        incoming_update.*
    FROM incoming_update
    
    UNION ALL
        
    SELECT  
        NULL as mergeKey1,
        incoming_update.*
    FROM incoming_update
    JOIN {{ this }} AS tgt ON 
        incoming_update.{{ surrogate_key }} = tgt.{{ surrogate_key }}
    WHERE tgt.meta_is_current = 1
    
) 
AS source ON 
    target.{{ surrogate_key }} = mergeKey1
WHEN MATCHED AND target.meta_is_current = 1 THEN
    UPDATE SET 
        target.meta_is_current = 0,
        {% if truncate_meta_date %}
            target.meta_valid_to = TO_TIMESTAMP(TO_DATE(source.{% if aliases is not none and time_column in aliases %}{{aliases.get(time_column)}}{%- else -%}{{ time_column }}{% endif %}))
        {% else %}
            target.meta_valid_to = TO_TIMESTAMP(source.{% if aliases is not none and time_column in aliases %}{{aliases.get(time_column)}}{%- else -%}{{ time_column }}{% endif %})
        {% endif %}

WHEN NOT MATCHED THEN
    INSERT
        (
            {{ surrogate_key }},
            {% for col in table_columns %}{% if aliases is not none and col.name in aliases %}{{aliases.get(col.name)}}{%- else -%}{{ col.name }}{% endif %},
            {% endfor %}{% if add_meta_exec_date %}meta_exec_date,
            {% endif %}meta_valid_from,
            meta_valid_to,
            meta_is_current
        )
    VALUES
        (
            source.{{ surrogate_key }},
            {% for col in table_columns %}{% if aliases is not none and col.name in aliases %}source.{{aliases.get(col.name)}}{%- else -%}source.{{ col.name }}{% endif %}, 
            {% endfor %}{% if add_meta_exec_date %}source.meta_exec_date,
            {% endif %}source.meta_valid_from,
            source.meta_valid_to,
            source.meta_is_current
        )

{%- endmacro %}