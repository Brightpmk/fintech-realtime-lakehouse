{% set lookback_hours = var('gold_incremental_lookback_hours', 48) %}
{% set destination_partition_predicate %}
    (
        DBT_INTERNAL_DEST."year" * 1000000
        + DBT_INTERNAL_DEST."month" * 10000
        + DBT_INTERNAL_DEST."day" * 100
        + DBT_INTERNAL_DEST."hour"
    ) >= {{ lakehouse_cutoff_partition_hour_key(lookback_hours) }}
{% endset %}

{{
    config(
        materialized='incremental',
        unique_key='hourly_liquidity_key',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        predicates=[destination_partition_predicate | trim],
        properties={
            "format": "'PARQUET'",
            "format_version": "2",
            "compression_codec": "'ZSTD'",
            "partitioning": "ARRAY['year', 'month', 'day', 'hour']"
        }
    )
}}

with silver_transactions as (
    select
        s.transaction_id,
        s.amount,
        s.currency,
        s.is_flagged_suspicious,
        s.event_time,
        s."year" as year,
        s."month" as month,
        s."day" as day,
        s."hour" as hour
    from {{ source('silver', 'transactions') }} as s
    where s.event_time is not null
      and s.currency is not null
      and s.amount is not null
      and s."year" is not null
      and s."month" is not null
      and s."day" is not null
      and s."hour" is not null
      {% if var('gold_start_partition_key', none) is not none %}
      and {{ lakehouse_partition_hour_key('s') }} >= {{ var('gold_start_partition_key') }}
      {% endif %}
      {% if var('gold_end_partition_key', none) is not none %}
      and {{ lakehouse_partition_hour_key('s') }} <= {{ var('gold_end_partition_key') }}
      {% endif %}
      {% if is_incremental() %}
      and {{ lakehouse_partition_hour_key('s') }} >= {{ lakehouse_cutoff_partition_hour_key(lookback_hours) }}
      {% endif %}
),

hourly_aggregates as (
    select
        cast(date_trunc('hour', event_time) as timestamp(6)) as hour_bucket,
        currency,
        count(*) as transaction_count,
        count_if(is_flagged_suspicious) as suspicious_transaction_count,
        sum(amount) as total_amount,
        sum(case when is_flagged_suspicious then cast(0.00 as decimal(18,2)) else amount end) as net_amount,
        year,
        month,
        day,
        hour
    from silver_transactions
    group by
        cast(date_trunc('hour', event_time) as timestamp(6)),
        currency,
        year,
        month,
        day,
        hour
)

select
    concat(date_format(hour_bucket, '%Y-%m-%d %H:%i:%s'), '|', currency) as hourly_liquidity_key,
    hour_bucket,
    currency,
    transaction_count,
    suspicious_transaction_count,
    total_amount,
    net_amount,
    year,
    month,
    day,
    hour,
    cast(current_timestamp as timestamp(6)) as model_updated_at
from hourly_aggregates