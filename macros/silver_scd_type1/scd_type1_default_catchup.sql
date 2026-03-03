{% macro scd_type1_default_catchup(
    src_table=none,
    surrogate_key=none,
    primary_keys=none,
    time_column=none,
    exec_date=none,
    aliases=none,
    column_rename=none,
    exclusion_columns=none,
    add_meta_exec_date=True,
    row_hash_column="meta_row_hash"
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
    {% if row_hash_column is none %}
        {% do exceptions.raise_compiler_error('Error: row_hash_column must be defined!') %}
    {% endif %}
{% endif %}

{% set table_columns = adapter.get_columns_in_relation(src_table) %}

{# -- IF SURROGATE_KEY IS REQUIRED
   -- SURROGATE_KEY ONLY ALLOWED IF THERE IS MORE THAN 1 PK 
 #}
 {% set pk_list = primary_keys if primary_keys is iterable and primary_keys is not string else [primary_keys] %}

{% if execute and surrogate_key is not none and (pk_list | length) <= 1 %}
    {% do exceptions.raise_compiler_error(
        'Error: surrogate_key can only be provided when there is more than one primary key. ' ~
        'With a single PK you must run without surrogate_key.'
    ) %}
{% endif %}

{% set has_suk = surrogate_key is not none and (pk_list | length) > 1 %}

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

WITH base_incoming AS (
    SELECT
        {% for col in table_columns %}
            {%- if column_rename is not none and col.name in column_rename -%}
                {{ column_rename.get(col.name) }}
            {% elif aliases is not none and col.name in aliases %}
                {{ col.name }} AS {{aliases.get(col.name)}}
            {%- else -%}
                {{ col.name }}
            {% endif %}, 
        {% endfor %}
        {% if add_meta_exec_date %}
            CURRENT_TIMESTAMP() AS meta_exec_date
        {% endif %}
    FROM {{ src_table }}
    WHERE {{time_column}} = '{{exec_date}}'
)


{# -- CASO PRIMARY_KEYS SEJA RECEBIDO COMO LISTA, ALTERAR PARA:
{% set pk_list = primary_keys if primary_keys is iterable and primary_keys is not string else [primary_keys] %}
#}

incoming_update AS (

    SELECT
        {# -- CALCULATES SURROGATE_KEY IF REQUIRED
        #}
        {% if has_suk %}
            xxhash64(
                upper(
                    trim(
                        concat_ws(',',
                        {% for pk in primary_keys %}
                            {% set pk_name = aliases.get(pk) if aliases is not none and pk in aliases else pk %}
                            cast({{ pk_name }} as string)
                            {%- if not loop.last -%},{%- endif -%}
                        {% endfor %}
                        )
                    )
                )
            ) AS {{ surrogate_key }},
        {% endif %}
        base_incoming.*
    FROM base_incoming
)

MERGE INTO {{ this }} AS target
USING incoming_update as source
ON
    {% if primary_keys is iterable %}
        {% for pk in primary_keys -%}
            target.{{ pk }} = source.{{ pk }}
            {% if not loop.last %} 
                AND 
            {% endif %}
        {% endfor %}
    {% endif %}
    
WHEN MATCHED AND
    {% if aliases is not none and row_hash_column in aliases -%}
        source.{{ aliases.get(row_hash_column) }} <> target.{{ aliases.get(row_hash_column) }}
    {% else -%}
        source.{{ row_hash_column }} <> target.{{ row_hash_column }}
    {% endif -%}
THEN UPDATE SET
        {% for col in table_columns -%}
            {% if col.name not in primary_keys -%}
                {% if aliases is not none and col.name in aliases -%}
                    {{ aliases.get(col.name) }} = source.{{ aliases.get(col.name) }}
                {% else -%}
                    {{ col.name }} = source.{{ col.name }}
                {% endif -%},
            {% endif -%}
        {% endfor %}
        {% if add_meta_exec_date %}
            meta_exec_date = source.meta_exec_date
        {% endif %}

WHEN NOT MATCHED THEN
    INSERT (
        {% if has_suk %}
            {{ surrogate_key }},
        {% endif %}
        {% for col in table_columns -%}
            {% if aliases is not none and col.name in aliases -%}
                {{ aliases.get(col.name) }}
            {% else -%}
                {{ col.name }}
            {% endif -%},
        {% endfor %}
        {% if add_meta_exec_date %}
            meta_exec_date
        {% endif %}
    )
    VALUES (
        {% if has_suk %}
            source.{{ surrogate_key }},
        {% endif %}
        {% for col in table_columns -%}
            {% if aliases is not none and col.name in aliases -%}
                source.{{ aliases.get(col.name) }}
            {% else -%}
                source.{{ col.name }}
            {% endif -%},
        {% endfor %}
        {% if add_meta_exec_date %}
            source.meta_exec_date
        {% endif %}
    )

{%- endmacro %}