{% macro _scd2_pk_in_incoming(pk, aliases) -%}
  {%- if aliases is not none and pk in aliases -%}
    {{ aliases.get(pk) }}
  {%- else -%}
    {{ pk }}
  {%- endif -%}
{%- endmacro %}

{% macro scd_type2_soft_delete_catchup(
    src_table=none,
    surrogate_key=none, 
    primary_keys=none,
    time_column=none,
    exec_date=none,
    aliases=none,
    column_rename=none,
    exclusion_columns=none,
    add_meta_exec_date=True,
    truncate_meta_date=False,
    handle_deletes=True,
    delete_close_strategy="same_day",
    columns_override=none
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

{% set pk_list = primary_keys if primary_keys is iterable and primary_keys is not string else [primary_keys] %}
{% if columns_override is not none %}
  {% set table_columns = [] %}
  {% for c in (columns_override if columns_override is iterable and columns_override is not string else [columns_override]) %}
    {% do table_columns.append({'name': c}) %}
  {% endfor %}
{% else %}
  {% set table_columns = adapter.get_columns_in_relation(src_table) %}
{% endif %}

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


{% set incoming_select_cols -%}
    {%- for col in table_columns -%}
        {%- if column_rename is not none and col.name in column_rename -%}
            {{ column_rename.get(col.name) }}
        {%- elif aliases is not none and col.name in aliases -%}
            {{ col.name }} AS {{ aliases.get(col.name) }}
        {%- else -%}
            {{ col.name }}
        {%- endif -%}
        {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
{%- endset %}


{% set target_insert_cols -%}
    {% if has_suk %}
        {{ surrogate_key }},
    {% endif %}
    {%- for col in table_columns -%}
        {%- if aliases is not none and col.name in aliases -%}
            {{ aliases.get(col.name) }}
        {%- else -%}
            {{ col.name }}
        {%- endif -%}
        , 
    {%- endfor -%}
    {%- if add_meta_exec_date -%}meta_exec_date,{%- endif -%}
    meta_valid_from, meta_valid_to, meta_is_current
{%- endset %}

{% set source_values_cols -%}
    {% if has_suk %}
        source.{{ surrogate_key }},
    {% endif %}
    {%- for col in table_columns -%}
        {%- if aliases is not none and col.name in aliases -%}
            source.{{ aliases.get(col.name) }}
        {%- else -%}
            source.{{ col.name }}
        {%- endif -%}
        , 
    {%- endfor -%}
    {%- if add_meta_exec_date -%}source.meta_exec_date,{%- endif -%}
    source.meta_valid_from, source.meta_valid_to, source.meta_is_current
{%- endset %}

{%- set effective_time_col = (aliases.get(time_column) if aliases is not none and time_column in aliases else time_column) -%}
{%- set open_ended_ts = "TO_TIMESTAMP('9999-12-31 00:00:00', 'yyyy-MM-dd HH:mm:ss')" -%}


CREATE OR REPLACE TEMP VIEW base_incoming AS
SELECT
{{ incoming_select_cols }},
{%- if add_meta_exec_date %}
    CURRENT_TIMESTAMP() AS meta_exec_date,
{%- endif %}
{%- if truncate_meta_date %}
    TO_TIMESTAMP(TO_DATE({{ effective_time_col }})) AS meta_valid_from,
{%- else %}
    TO_TIMESTAMP({{ effective_time_col }}) AS meta_valid_from,
{%- endif %}
{{ open_ended_ts }} AS meta_valid_to,
1 AS meta_is_current
FROM {{ src_table }}
WHERE {{ time_column }} = '{{ exec_date }}'
;


CREATE OR REPLACE TEMP VIEW incoming_update AS
SELECT
    {# -- CALCULATES SURROGATE_KEY IF REQUIRED
    #}
    {% if has_suk %}
        xxhash64(
            concat(
                    upper(
                        trim(
                            concat_ws(',',
                                {% for pk in pk_list %}
                                    {% set pk_name = aliases.get(pk) if aliases is not none and pk in aliases else pk %}
                                    cast({{ pk_name }} as string)
                                    {%- if not loop.last -%},{%- endif -%}
                                {% endfor %}
                                )
                            )
                        ),
                        '|',
                        date_format(meta_valid_from, 'yyyy-MM-dd HH:mm:ss')
                    )
            ) AS {{ surrogate_key }},
    {% endif %}
    base_incoming.*
FROM base_incoming
;

CREATE OR REPLACE TEMP VIEW incoming_keys AS
SELECT DISTINCT
    {% for pk in pk_list %}
        {{ _scd2_pk_in_incoming(pk, aliases) }} AS {{ _scd2_pk_in_incoming(pk, aliases) }}{% if not loop.last %}, {% endif %}
    {% endfor %}
FROM incoming_update
;

MERGE INTO {{ this }} AS target
USING (

    SELECT
        {% for pk in pk_list %}
            incoming_update.{{ _scd2_pk_in_incoming(pk, aliases) }} AS mergeKey{{ loop.index }}{% if not loop.last %}, {% endif %}
        {% endfor %},
        incoming_update.*
    FROM incoming_update

    UNION ALL

    SELECT
        {% for pk in pk_list %}
            NULL AS mergeKey{{ loop.index }}{% if not loop.last %}, {% endif %}
        {% endfor %},
        incoming_update.*
    FROM incoming_update
    JOIN {{ this }} AS tgt
        ON
        {% for pk in pk_list %}
            incoming_update.{{ _scd2_pk_in_incoming(pk, aliases) }} = tgt.{{ _scd2_pk_in_incoming(pk, aliases) }}{% if not loop.last %} AND{% endif %}
        {% endfor %}
    WHERE tgt.meta_is_current = 1
        AND tgt.meta_valid_to = {{ open_ended_ts }}
        AND tgt.meta_row_hash <> incoming_update.meta_row_hash

) AS source
ON
    {% for pk in pk_list %}
        target.{{ _scd2_pk_in_incoming(pk, aliases) }} = source.mergeKey{{ loop.index }}{% if not loop.last %} AND{% endif %}
    {% endfor %}

{# -- SCD2 UPDATE: IF CURRENT TARGET ROW EXISTS FOR THE PK AND meta_row_hash CHANGED
   -- AND CLOSES THE CURRENT ROW (meta_is_current = 0) AND SET meta_valid_to TO JUST BEFORE THE NEW VERSION STARTS.
#}
WHEN MATCHED
    AND target.meta_is_current = 1
    AND target.meta_valid_to = {{ open_ended_ts }}
    AND target.meta_row_hash <> source.meta_row_hash
THEN UPDATE SET
    target.meta_is_current = 0,
    {%- if truncate_meta_date %}
        target.meta_valid_to = TO_TIMESTAMP(DATE_SUB(TO_DATE('{{ exec_date }}'), 1))

    {%- else %}
        target.meta_valid_to = (source.meta_valid_from - INTERVAL 1 MILLISECOND)
    {%- endif %}

{# -- SCD2 INSERT: INSERT NEW CURRENT VERSION WHEN THERE IS NO MATCHING TARGET ROW FOR THE PK.
#}
WHEN NOT MATCHED THEN
    INSERT ({{ target_insert_cols }})
    VALUES ({{ source_values_cols }})
;


{%- if handle_deletes %}

CREATE OR REPLACE TEMP VIEW deleted_keys AS
SELECT
    {% for pk in pk_list %}
        s.{{ _scd2_pk_in_incoming(pk, aliases) }} AS {{ _scd2_pk_in_incoming(pk, aliases) }}{% if not loop.last %}, {% endif %}
    {% endfor %}
FROM {{ this }} s
LEFT ANTI JOIN incoming_keys i
    ON
    {% for pk in pk_list %}
        s.{{ _scd2_pk_in_incoming(pk, aliases) }} = i.{{ _scd2_pk_in_incoming(pk, aliases) }}{% if not loop.last %} AND{% endif %}
    {% endfor %}
WHERE s.meta_is_current = 1
    AND s.meta_valid_to = {{ open_ended_ts }}
    AND s.meta_exec_date < TO_TIMESTAMP('{{ exec_date }}')
;

{# -- CLOSE RECORDS IDENTIFIED IN deleted_keys BY SETTING meta_valid_to ACCORDING TO delete_close_strategy. NOTE: meta_is_current IS KEPT AS 1 FOR SOFT-DELETED ROWS.
#}
UPDATE {{ this }}
SET
    meta_valid_to =
        {%- if delete_close_strategy == "day_before" -%}
            TO_TIMESTAMP('{{ exec_date }}') - INTERVAL 1 DAY
        {%- else -%}
            TO_TIMESTAMP('{{ exec_date }}') - INTERVAL 1 MILLISECOND
        {%- endif -%}
    {% if add_meta_exec_date %}
    , meta_exec_date = CURRENT_TIMESTAMP()
    {% endif %}
WHERE meta_is_current = 1
    AND meta_valid_to = {{ open_ended_ts }}
    AND EXISTS (
        SELECT 1
        FROM deleted_keys d
        WHERE
        {% for pk in pk_list %}
            {{ this }}.{{ _scd2_pk_in_incoming(pk, aliases) }} = d.{{ _scd2_pk_in_incoming(pk, aliases) }}{% if not loop.last %} AND{% endif %}
        {% endfor %}
    )
;

{%- endif %}

{%- endmacro %}
