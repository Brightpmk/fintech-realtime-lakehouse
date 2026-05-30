select *
from {{ ref('gold_hourly_liquidity') }}
where transaction_count <= 0
   or suspicious_transaction_count < 0
   or total_amount < cast(0.00 as decimal(18, 2))
   or net_amount < cast(0.00 as decimal(18, 2))
