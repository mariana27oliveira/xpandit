{% macro scd_type2_default_catchup(
    src_table=none, 
    primary_keys=none,
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
    {% if primary_keys is none %}
        {% do exceptions.raise_compiler_error('Error: primary_keys must be defined!') %}
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


WITH incoming_update AS (

    SELECT
        {% for col in table_columns %}{%- if column_rename is not none and col.name in column_rename -%}{{ column_rename.get(col.name) }}{% elif aliases is not none and col.name in aliases %}{{ col.name }} AS {{aliases.get(col.name)}}{%- else -%}{{ col.name }}{% endif %}, 
        {% endfor %}{% if add_meta_exec_date %}CURRENT_TIMESTAMP()  AS meta_exec_date,
        {% endif %}

        {% if truncate_meta_date %}
            TO_TIMESTAMP(TO_DATE({{ time_column }})) AS effective_start_date,
        {% else %}
            TO_TIMESTAMP({{ time_column }}) AS effective_start_date,
        {% endif %}

        TO_TIMESTAMP('31/12/9999 00:00:00', 'dd/MM/yyyy HH:mm:ss') AS effective_end_date,
        1 AS is_current
    FROM {{ src_table }}
    WHERE DATEADD(second, -1, DATEADD(day, 1, TO_TIMESTAMP('{{ exec_date }}'))) BETWEEN meta_valid_from AND meta_valid_to

)


MERGE INTO {{ this }} as target
USING (

    SELECT 
        {% for pk in primary_keys %}incoming_update.{{ pk }} as mergeKey{{loop.index}},
        {% endfor %}incoming_update.*
    FROM incoming_update
    
    UNION ALL
        
    SELECT  
        {% for pk in primary_keys %}NULL as mergeKey{{loop.index}},
        {% endfor %}incoming_update.*
    FROM incoming_update
    JOIN {{ this }} AS tgt ON 
        {% for pk in primary_keys %}incoming_update.{{ pk }} = tgt.{{ pk }} {% if not loop.last %}AND
        {% endif %}{% endfor %}
    WHERE tgt.is_current = 1 AND tgt.meta_row_hash <> incoming_update.meta_row_hash
    
) 
AS source ON 
    {% for pk in primary_keys %}target.{{ pk }} = mergeKey{{loop.index}} {% if not loop.last %}AND{% endif %} 
    {% endfor %}
WHEN MATCHED AND target.is_current = 1 AND target.meta_row_hash <> source.meta_row_hash THEN
    UPDATE SET 
        target.is_current = 0,
        {% if truncate_meta_date %}
            target.effective_end_date = TO_TIMESTAMP(TO_DATE(source.{% if aliases is not none and time_column in aliases %}{{aliases.get(time_column)}}{%- else -%}{{ time_column }}{% endif %}) - INTERVAL 1 DAY)
        {% else %}
            target.effective_end_date = dateadd(millisecond, -1, TO_TIMESTAMP(source.{% if aliases is not none and time_column in aliases %}{{aliases.get(time_column)}}{%- else -%}{{ time_column }}{% endif %}))
        {% endif %}
        

WHEN NOT MATCHED THEN
    INSERT
        (
            {% for col in table_columns %}{% if aliases is not none and col.name in aliases %}{{aliases.get(col.name)}}{%- else -%}{{ col.name }}{% endif %},
            {% endfor %}{% if add_meta_exec_date %}meta_exec_date,
            {% endif %}effective_start_date,
            effective_end_date,
            is_current
        )
    VALUES
        (
            {% for col in table_columns %}{% if aliases is not none and col.name in aliases %}source.{{aliases.get(col.name)}}{%- else -%}source.{{ col.name }}{% endif %}, 
            {% endfor %}{% if add_meta_exec_date %}source.meta_exec_date,
            {% endif %}source.effective_start_date,
            source.effective_end_date,
            source.is_current
        )

{%- endmacro %}