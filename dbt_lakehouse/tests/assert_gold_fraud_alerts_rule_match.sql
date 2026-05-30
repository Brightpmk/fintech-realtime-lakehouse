select *
from {{ ref('gold_fraud_alerts') }}
where not (
    is_flagged_suspicious
    or amount > cast(10000.00 as decimal(18, 2))
    or (
        amount > cast(5000.00 as decimal(18, 2))
        and regexp_like(lower(coalesce(location, '')), 'lagos|sao paulo|new york|bangkok')
    )
)
