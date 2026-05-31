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
        unique_key='transaction_id',
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

with raw_silver as (
    select
        s.transaction_id,
        s.account_id,
        s.device_id,
        s.amount,
        s.currency,
        s.location,
        s.is_flagged_suspicious,
        s.event_time,
        s.event_time_epoch_us,
        s."year" as year,
        s."month" as month,
        s."day" as day,
        s."hour" as hour,
        row_number() over (
            partition by s.transaction_id
            order by s.event_time desc, s.ingest_time desc
        ) as row_num
    from {{ source('silver', 'transactions') }} as s
    where s.transaction_id is not null
      and s.event_time is not null
      and s.event_time_epoch_us is not null
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

silver_transactions as (
    select
        transaction_id,
        account_id,
        device_id,
        amount,
        currency,
        location,
        is_flagged_suspicious,
        event_time,
        event_time_epoch_us,
        year,
        month,
        day,
        hour
    from raw_silver
    where row_num = 1
),

classified as (
    select
        *,
        regexp_like(
            lower(coalesce(location, '')),
            'lagos|sao paulo|new york|bangkok'
        ) as is_high_risk_location
    from silver_transactions
),

alerts as (
    select
        concat(transaction_id, '|', cast(event_time_epoch_us as varchar)) as fraud_alert_key,
        transaction_id,
        account_id,
        device_id,
        amount,
        currency,
        location,
        is_flagged_suspicious,
        case
            when is_flagged_suspicious then 'SIMULATOR_FLAG'
            when amount > cast(10000.00 as decimal(18, 2)) and is_high_risk_location
                then 'HIGH_AMOUNT_HIGH_RISK_LOCATION'
            when amount > cast(10000.00 as decimal(18, 2)) then 'HIGH_AMOUNT'
            when amount > cast(5000.00 as decimal(18, 2)) and is_high_risk_location
                then 'ELEVATED_AMOUNT_HIGH_RISK_LOCATION'
            else 'RISK_RULE_MATCH'
        end as alert_reason,
        event_time,
        event_time_epoch_us,
        year,
        month,
        day,
        hour
    from classified
    where is_flagged_suspicious
       or amount > cast(10000.00 as decimal(18, 2))
       or (amount > cast(5000.00 as decimal(18, 2)) and is_high_risk_location)
),

deduped_alerts as (
    select 
        *,
        row_number() over (
            partition by transaction_id
            order by event_time desc
        ) as row_num
    from alerts
)

select
    fraud_alert_key,
    transaction_id,
    account_id,
    device_id,
    amount,
    currency,
    location,
    is_flagged_suspicious,
    alert_reason,
    event_time,
    event_time_epoch_us,
    year,
    month,
    day,
    hour,
    cast(current_timestamp as timestamp(6)) as model_updated_at
from deduped_alerts
where row_num = 1