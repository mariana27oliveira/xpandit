{% macro scd_type2_one_active_record_test(src_table=none, primary_keys=none) -%}

SELECT {% for pk in primary_keys %}{{ pk }},{% endfor %} Count(*) as dup
FROM {{ src_table }}
WHERE meta_is_last_move = 'Y'
GROUP BY {% for pk in primary_keys %}{{ pk }}{% if not loop.last %},{% endif %} 
    {% endfor %}
HAVING count(*) > 1

{%- endmacro %}