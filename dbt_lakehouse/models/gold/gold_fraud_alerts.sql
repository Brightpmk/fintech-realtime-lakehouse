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
    from {{ source('silver', 'transactions') }}
    where transaction_id is not null
      and event_time is not null
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
            when amount > 10000 and is_high_risk_location
                then 'HIGH_AMOUNT_HIGH_RISK_LOCATION'
            when amount > 10000 then 'HIGH_AMOUNT'
            when amount > 5000 and is_high_risk_location
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
       or amount > 10000
       or (amount > 5000 and is_high_risk_location)
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
    cast(current_timestamp as timestamp(3)) as model_updated_at
from alerts
