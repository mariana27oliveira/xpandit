-- SO CORRE PARTES ESPECIFICAS DO CODIGO, MAS QUAL É A UTILIDADE?


{% materialization custom_merge, adapter='databricks' %}

  {#
    {%- set identifier = model['alias'] -%}
    {% set existing_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier, needs_information=True) %}
  #}

  {{ run_hooks(pre_hooks) }}

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