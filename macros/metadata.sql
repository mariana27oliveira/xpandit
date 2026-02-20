{% macro add_metadata_columns() %}
    current_timestamp() as created_at,
    current_timestamp() as updated_at
{% endmacro %}