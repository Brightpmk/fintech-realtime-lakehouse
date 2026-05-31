{% macro lakehouse_partition_hour_key(alias, operator=none, value=none) -%}
    {%- if operator is not none and value is not none -%}
        {%- set Y = "cast((" ~ value ~ ") / 1000000 as bigint)" -%}
        {%- set M = "cast((" ~ value ~ ") / 10000 as bigint) % 100" -%}
        {%- set D = "cast((" ~ value ~ ") / 100 as bigint) % 100" -%}
        {%- set H = "cast(" ~ value ~ " as bigint) % 100" -%}
        
        {%- if operator == '>=' -%}
            (
                {{ alias }}."year" > {{ Y }}
                OR ({{ alias }}."year" = {{ Y }} AND {{ alias }}."month" > {{ M }})
                OR ({{ alias }}."year" = {{ Y }} AND {{ alias }}."month" = {{ M }} AND {{ alias }}."day" > {{ D }})
                OR ({{ alias }}."year" = {{ Y }} AND {{ alias }}."month" = {{ M }} AND {{ alias }}."day" = {{ D }} AND {{ alias }}."hour" >= {{ H }})
            )
        {%- elif operator == '<=' -%}
            (
                {{ alias }}."year" < {{ Y }}
                OR ({{ alias }}."year" = {{ Y }} AND {{ alias }}."month" < {{ M }})
                OR ({{ alias }}."year" = {{ Y }} AND {{ alias }}."month" = {{ M }} AND {{ alias }}."day" < {{ D }})
                OR ({{ alias }}."year" = {{ Y }} AND {{ alias }}."month" = {{ M }} AND {{ alias }}."day" = {{ D }} AND {{ alias }}."hour" <= {{ H }})
            )
        {%- else -%}
            (
                {{ alias }}."year" * 1000000
                + {{ alias }}."month" * 10000
                + {{ alias }}."day" * 100
                + {{ alias }}."hour"
            ) {{ operator }} {{ value }}
        {%- endif -%}
    {%- else -%}
        (
            {{ alias }}."year" * 1000000
            + {{ alias }}."month" * 10000
            + {{ alias }}."day" * 100
            + {{ alias }}."hour"
        )
    {%- endif -%}
{%- endmacro %}

{% macro lakehouse_cutoff_partition_hour_key(lookback_hours) -%}
    cast(
        date_format(
            date_add('hour', -{{ lookback_hours | int }}, at_timezone(current_timestamp, 'UTC')),
            '%Y%m%d%H'
        ) as integer
    )
{%- endmacro %}
