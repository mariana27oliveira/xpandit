-- Uses the table and column descriptions defined in the 'schema.yml' to generate SQL commands to comment the table and columns in Databricks

{% macro comments(model, table) %}
    {% if execute %}
        {%- set apply_comments = var('apply_comments', false) -%}

        {% if apply_comments %}
            {%- set model_node = graph.nodes.get(model.unique_id) -%}
            {%- set table_description = model_node.get('description', NULL) -%}

            {%- set queries = [] -%}

            {%- set safe_table_description = table_description | replace("'", "''") -%}
            {%- do queries.append("COMMENT ON TABLE " ~ table ~ " IS '" ~ safe_table_description ~ "'") -%}

            {%- set columns = model_node.get('columns', {}) -%}
            {% for column_name, column_info in columns.items() %}
                {% if column_info.description is defined %}
                    {%- set safe_column_description = column_info.description | replace("'", "''") -%}
                    {%- do queries.append("COMMENT ON COLUMN " ~ table ~ "." ~ column_name ~ " IS '" ~ safe_column_description ~ "'") -%}
                {% endif %}
            {% endfor %}

            {% for query in queries %}
                {% do run_query(query) %}
            {% endfor %}
        {% endif %}
    {% endif %}
{% endmacro %}