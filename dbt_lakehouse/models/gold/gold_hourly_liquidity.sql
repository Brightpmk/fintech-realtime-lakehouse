{% set lookback_hours = var('gold_incremental_lookback_hours', 48) %}
{% set dest_pred = lakehouse_partition_hour_key('DBT_INTERNAL_DEST', '>=', lakehouse_cutoff_partition_hour_key(lookback_hours)) %}

{{
    config(
        materialized='incremental', unique_key='hourly_liquidity_key', incremental_strategy='merge',
        on_schema_change='append_new_columns', predicates=[dest_pred | trim],
        properties={"format": "'PARQUET'", "format_version": "2", "compression_codec": "'ZSTD'", "partitioning": "ARRAY['year', 'month', 'day', 'hour']"}
    )
}}

select
    concat(cast(cast(to_unixtime(cast(cast(date_trunc('hour', s.event_time) as timestamp(6)) as timestamp(6) with time zone)) as bigint) as varchar), '|', s.currency) as hourly_liquidity_key,
    cast(date_trunc('hour', s.event_time) as timestamp(6)) as hour_bucket,
    s.currency,
    count(*) as transaction_count,
    count_if(s.is_flagged_suspicious) as suspicious_transaction_count,
    sum(s.amount) as total_amount,
    sum(case when not s.is_flagged_suspicious then s.amount else cast(0.00 as decimal(18,2)) end) as net_amount,
    s.year, s.month, s.day, s.hour,
    cast(current_timestamp as timestamp(6)) as model_updated_at
from {{ source('silver', 'transactions') }} as s
where s.event_time is not null and s.currency is not null and s.amount is not null
  and s.year is not null and s.month is not null and s.day is not null and s.hour is not null
  {% if var('gold_start_partition_key', none) is not none %}
  and {{ lakehouse_partition_hour_key('s', '>=', var('gold_start_partition_key')) }}
  {% endif %}
  {% if var('gold_end_partition_key', none) is not none %}
  and {{ lakehouse_partition_hour_key('s', '<=', var('gold_end_partition_key')) }}
  {% endif %}
  {% if is_incremental() %}
  and {{ lakehouse_partition_hour_key('s', '>=', lakehouse_cutoff_partition_hour_key(lookback_hours)) }}
  {% endif %}
group by
    cast(date_trunc('hour', s.event_time) as timestamp(6)),
    s.currency, s.year, s.month, s.day, s.hour