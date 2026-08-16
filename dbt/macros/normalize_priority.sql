{% macro normalize_priority(raw_expr) -%}
case
    when try_cast({{ raw_expr }} as integer) between 1 and 4
        then try_cast({{ raw_expr }} as integer)
    when lower(trim({{ raw_expr }})) = 'urgent'  then 1
    when lower(trim({{ raw_expr }})) = 'high'    then 2
    when lower(trim({{ raw_expr }})) = 'medium'  then 3
    when lower(trim({{ raw_expr }})) = 'low'     then 4
    else null
end
{%- endmacro %}
