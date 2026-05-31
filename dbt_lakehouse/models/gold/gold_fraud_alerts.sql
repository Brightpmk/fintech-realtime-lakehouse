{% set lookback_hours = var('gold_incremental_lookback_hours', 48) %}
{% set dest_pred = lakehouse_partition_hour_key('DBT_INTERNAL_DEST', '>=', lakehouse_cutoff_partition_hour_key(lookback_hours)) %}

{{
    config(
        materialized='incremental', unique_key='transaction_id', incremental_strategy='merge',
        on_schema_change='append_new_columns', predicates=[dest_pred | trim],
        properties={"format": "'PARQUET'", "format_version": "2", "compression_codec": "'ZSTD'", "partitioning": "ARRAY['year', 'month', 'day', 'hour']"}
    )
}}

select
    s.transaction_id as fraud_alert_key,
    s.transaction_id, s.account_id, s.device_id, s.amount, s.currency, s.location, s.is_flagged_suspicious,
    case
        when s.is_flagged_suspicious then 'SIMULATOR_FLAG'
        when s.amount > 10000.00 and regexp_like(lower(coalesce(s.location, '')), 'lagos|sao paulo|new york|bangkok') then 'HIGH_AMOUNT_HIGH_RISK_LOCATION'
        when s.amount > 10000.00 then 'HIGH_AMOUNT'
        when s.amount > 5000.00 and regexp_like(lower(coalesce(s.location, '')), 'lagos|sao paulo|new york|bangkok') then 'ELEVATED_AMOUNT_HIGH_RISK_LOCATION'
        else 'RISK_RULE_MATCH'
    end as alert_reason,
    s.event_time, s.event_time_epoch_us, s.year, s.month, s.day, s.hour,
    cast(current_timestamp as timestamp(6)) as model_updated_at
from {{ source('silver', 'transactions') }} as s
where s.transaction_id is not null and s.event_time is not null and s.event_time_epoch_us is not null
  and s.amount is not null and s.year is not null and s.month is not null and s.day is not null and s.hour is not null
  and (s.is_flagged_suspicious or s.amount > 10000.00 or (s.amount > 5000.00 and regexp_like(lower(coalesce(s.location, '')), 'lagos|sao paulo|new york|bangkok')))
  {% if var('gold_start_partition_key', none) is not none %}
  and {{ lakehouse_partition_hour_key('s', '>=', var('gold_start_partition_key')) }}
  {% endif %}
  {% if var('gold_end_partition_key', none) is not none %}
  and {{ lakehouse_partition_hour_key('s', '<=', var('gold_end_partition_key')) }}
  {% endif %}
  {% if is_incremental() %}
  and {{ lakehouse_partition_hour_key('s', '>=', lakehouse_cutoff_partition_hour_key(lookback_hours)) }}
  {% endif %}