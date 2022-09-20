{% materialization table, adapter='datafusion' %}

  {%- set target_relation = this.incorporate(type='table') -%}

  {% call statement('main') %}
    {{ adapter.create_table_as(model.name, sql) }}
  {% endcall %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
