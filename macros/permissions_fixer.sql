{% macro reset_permissions(table) %}

  {% set query %}
    GRANT ALL PRIVILEGES ON TABLE {{table}} TO vidyarddbt
  {% endset %}

  {% do run_query(query) %}
{% endmacro %}