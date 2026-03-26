{% macro extract_param(param_key, value_type='string') %}
    max(case when param_key = '{{ param_key }}' then
        {% if value_type == 'string' %}
            param_string_value
        {% elif value_type == 'int' %}
            param_int_value
        {% elif value_type == 'float' %}
            param_float_value
        {% endif %}
    end) as {{ param_key }}
{% endmacro %}