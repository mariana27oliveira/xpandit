{% materialization custom_merge, adapter='databricks' %}

  {#
    {%- set identifier = model['alias'] -%}
    {% set existing_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier, needs_information=True) %}
  #}

  {{ run_hooks(pre_hooks) }}

  {# -- MULTI-STATEMENT MODE -> SOME ADAPTERS / EXECUTION CONTEXTS DO NOT ACCEPT MULTIPLE SQL STATEMENTS IN A SINGLE EXECUTION CALL. ENABLE THIS FOR MODELS THAT EMIT MORE THAN ONE STATEMENT (E.G., MERGE + CREATE VIEW/UPDATE, EXAMPLE: soft_delete).
  #}
  {% set multi_statement = config.get('multi_statement', false) %}

  {% do log("custom_merge multi_statement=" ~ (config.get('multi_statement', false) | string), info=true) %}

  {% if not multi_statement %}

    {% call statement('main') %}
      {# ---- DEACTIVATED: CREATE TABLE IF FIRST RUN. DISABLED DUE TO HARDCODED VALUES & GOLD LAYER PROBLEMS
      {%- if existing_relation is none -%}

        {% set merge_index = sql.upper().find('MERGE') %}
        {% if merge_index != -1 %}
            {% set cte_select = sql[:merge_index] %}
        {% else %}
            {% set cte_select = sql %}
        {% endif %}

        {% set create_sql = "CREATE TABLE IF NOT EXISTS "~this.render()~" AS "~cte_select~" SELECT * FROM incoming_update" %} -- HARDCODED incoming_update name
        {{ create_sql }}
        
      {% else %}
    #}
      
      {{ sql }}
      
      {#{%- endif -%}#}
    {% endcall %}

  {% else %}
  {# -- MULTI-STATEMENT EXECUTION -> SPLITS SQL BY ';' AND EXECUTES EACH STATEMENT INDIVIDUALLY.
  #}
    {% if execute %}
      {# -- SPLIT SQL STATEMENTS USING ';' (ASSUMES ';' IS NOT USED INSIDE STRINGS).
      #}
      {% set parts = sql.split(';') %}
      {% set non_empty = [] %}
      {% for p in parts %}
        {% set cleaned = p.strip() %}
        {% if cleaned %}
          {% do non_empty.append(cleaned) %}
        {% endif %}
      {% endfor %}

      {# -- 1ST STATEMENT MUST USE statement('main') (DBT ALWAYS EXPECTS A 'main' STATEMENT).
      #}
      {% for stmt in non_empty %}
        {% if loop.first %}
          {% call statement('main') %}
            {{ stmt }}
          {% endcall %}
        {% else %}
          {% call statement('stmt_' ~ loop.index) %}
            {{ stmt }}
          {% endcall %}
        {% endif %}
      {% endfor %}
    {% endif %}

  {% endif %}


  {# --- DEACTIVATED: Implicit OPTIMIZE & VACUUM. DISABLED IN FAVOR OF EXPLICT USE IN MODEL
  {% call statement('optimize') %}
    OPTIMIZE {{ this.render() }}
  {% endcall %}

  {% call statement('vacuum') %}
    VACUUM {{ this.render() }}
  {% endcall %}
  #}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [this]})}}
{% endmaterialization %}