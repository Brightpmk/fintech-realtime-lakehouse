{% macro lakehouse_partition_hour_key(alias) -%}
    (
        {{ alias }}."year" * 1000000
        + {{ alias }}."month" * 10000
        + {{ alias }}."day" * 100
        + {{ alias }}."hour"
    )
{%- endmacro %}

{% macro lakehouse_cutoff_partition_hour_key(lookback_hours) -%}
    cast(
        date_format(
            date_add('hour', -{{ lookback_hours | int }}, at_timezone(current_timestamp, 'UTC')),
            '%Y%m%d%H'
        ) as integer
    )
{%- endmacro %}
