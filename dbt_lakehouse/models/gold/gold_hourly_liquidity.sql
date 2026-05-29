{{
    config(
        materialized='table',
        schema='gold',
        on_table_exists='drop',
        properties={
            "format": "'PARQUET'",
            "format_version": "2",
            "partitioning": "ARRAY['year', 'month', 'day']"
        }
    )
}}

with silver_transactions as (
    select
        transaction_id,
        amount,
        currency,
        is_flagged_suspicious,
        event_time,
        year,
        month,
        day,
        hour
    from {{ source('silver', 'transactions') }}
    where event_time is not null
      and currency is not null
      and amount is not null
      and year is not null
      and month is not null
      and day is not null
      and hour is not null
      {% if var('gold_start_partition_key', none) is not none %}
      and ((year * 1000000) + (month * 10000) + (day * 100) + hour)
          >= {{ var('gold_start_partition_key') }}
      {% endif %}
      {% if var('gold_end_partition_key', none) is not none %}
      and ((year * 1000000) + (month * 10000) + (day * 100) + hour)
          <= {{ var('gold_end_partition_key') }}
      {% endif %}
),

hourly_aggregates as (
    select
        cast(date_trunc('hour', event_time) as timestamp(3)) as hour_bucket,
        currency,
        count(*) as transaction_count,
        count_if(is_flagged_suspicious) as suspicious_transaction_count,
        sum(amount) as total_amount,
        sum(amount) as net_amount,
        year,
        month,
        day,
        hour
    from silver_transactions
    group by
        cast(date_trunc('hour', event_time) as timestamp(3)),
        currency,
        year,
        month,
        day,
        hour
)

select
    concat(cast(hour_bucket as varchar), '|', currency) as hourly_liquidity_key,
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
    cast(current_timestamp as timestamp(3)) as model_updated_at
from hourly_aggregates
