{% macro parse_json_field(json_field, path) %}
    TRY_PARSE_JSON({{ json_field }}):{{ path }}::STRING
{% endmacro %}