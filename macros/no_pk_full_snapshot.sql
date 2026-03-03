{% macro no_pk_full_snapshot(
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


{# -- VALIDATIONS -- 
#}
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

{# -- SOURCE COLUMNS INTROSPECTION 
#}
{% set table_columns = adapter.get_columns_in_relation(src_table) %}

{# -- OPTIONAL COLUMN EXCLUSION
#}
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

{# -- BUSINESS COLUMNS ONLY
#}
{% set table_columns_no_metadata = [] %}
{% for col in table_columns %}
    {% if not col.name.startswith("meta") %}
        {% do table_columns_no_metadata.append(col) %}
    {% endif %}
{% endfor %}

{# -- BUILD INCOMING FIELD METADATA:
    -- SELECT_EXPR: HOW TO SELECT FROM SOURCE (WITH RENAME/ALIAS)
    -- OUT_NAME: FINAL COLUMN NAME IN SILVER
    -- DATA_TYPE: USED TO CAST NULLS IN UNION ALL
#}
{% set brz_fields = [] %}

{% for col in table_columns %}
  {% if column_rename is not none and col.name in column_rename %}
    {% set out_name = column_rename.get(col.name) %}
    {% set select_expr = col.name ~ " AS " ~ out_name %}
  {% elif aliases is not none and col.name in aliases %}
    {% set out_name = aliases.get(col.name) %}
    {% set select_expr = col.name ~ " AS " ~ out_name %}
  {% else %}
    {% set out_name = col.name %}
    {% set select_expr = col.name %}
  {% endif %}

  {% do brz_fields.append({
    "src_name": col.name,
    "out_name": out_name,
    "data_type": col.data_type,
    "select_expr": select_expr
  }) %}
{% endfor %}

{# -- EXECUTION METADATA FOR THE LOAD
#}
WITH params AS (
  SELECT CURRENT_TIMESTAMP() AS created_at
),

{# -- SNAPSHOT FOR THE GIVEN EXECUTION DATE
#}
incoming_update AS (
    SELECT
        xxhash64(
            {%- for col in table_columns_no_metadata -%}
                {{ col.name }}{%- if not loop.last -%},{%- endif -%}
            {%- endfor -%}
        ) AS {{ surrogate_key }},
        {%- for f in brz_fields -%}
            {{ f.select_expr }}{%- if not loop.last -%},{%- endif -%}
        {%- endfor -%}
        {% if add_meta_exec_date -%}
            , CURRENT_TIMESTAMP() AS meta_exec_date
        {% endif -%}
        , 1 AS meta_is_current
        , (SELECT created_at FROM params) AS meta_created_at
    FROM {{ src_table }}
    WHERE {{ time_column }} = '{{ exec_date }}'
),

{# -- ENUMERATE CURRENT ROWS IN TARGET TO BE CLOSED
#}
close_current AS (
  SELECT tgt.{{ surrogate_key }} AS close_sk
  FROM {{ this }} tgt
  WHERE tgt.meta_is_current = 1
),

{# -- MERGE SOURCE:
    -- INCOMING SNAPSHOT ROWS
    -- ONE ROW PER CURRENT TARGET ROW
#}
merge_source AS (
  SELECT
    i.*,
    CAST(NULL AS BIGINT) AS close_sk
  FROM incoming_update i

  UNION ALL

  SELECT
    CAST(NULL AS BIGINT) AS {{ surrogate_key }},
    {% for f in brz_fields %}
        CAST(NULL AS {{ f.data_type }}) AS {{ f.out_name }}{%- if not loop.last -%},{%- endif -%}
    {% endfor %}
    {%- if add_meta_exec_date -%}, CAST(NULL AS TIMESTAMP) AS meta_exec_date{%- endif -%}
    , CAST(NULL AS INT)       AS meta_is_current
    , CAST(NULL AS TIMESTAMP) AS meta_created_at
    , c.close_sk              AS close_sk
  FROM close_current c
)

{# -- APPLY CHANGES:
    -- CLOSE PREVIOUS CURRENT ROWS
    -- INSERT NEW SNAPSHOT ROWS
#}
MERGE INTO {{ this }} AS tgt
USING merge_source AS src
ON (tgt.meta_is_current = 1 AND tgt.{{ surrogate_key }} = src.close_sk)

WHEN MATCHED THEN
  UPDATE SET tgt.meta_is_current = 0

WHEN NOT MATCHED THEN
  INSERT (
    {{ surrogate_key }},
    {%- for f in brz_fields -%}
      {{ f.out_name }}{%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    {%- if add_meta_exec_date -%}, meta_exec_date{%- endif -%}
    , meta_is_current
    , meta_created_at
  )
  VALUES (
    src.{{ surrogate_key }},
    {%- for f in brz_fields -%}
      src.{{ f.out_name }}{%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    {%- if add_meta_exec_date -%}, src.meta_exec_date{%- endif -%}
    , src.meta_is_current
    , src.meta_created_at
  )


{%- endmacro %}