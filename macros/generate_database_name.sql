-- Replaces the catalog name creation to add a suffix equal to the target.name (ex: transforms the catalog 'catalog_bronze' with the target 'dev' to 'catalog_bronze_dev').
-- Can be used as to not have to change code between environments.
-- To use it, set the default catalog in the profiles.yml to the bronze layer catalog without the environment suffix. Ex: 'catalog_bronze'
-- For the silver and gold layer models override their catalogs to their equivalents of 'catalog_silver' and 'catalog_gold'
-- Uncomment this macro and DBT will use it by default

{% macro generate_database_name(custom_database_name=none, node=none) -%}

    {%- set default_database = target.database -%}
    {%- if custom_database_name is none -%}

        {{ default_database }}_{{ target.name }}

    {%- else -%}

        {{ custom_database_name | trim }}_{{ target.name }}

    {%- endif -%}

{%- endmacro %}
