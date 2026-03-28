-- Fails if result label does not match the sign of pnl

select *
from {{ ref('stg_trades') }}
where
    (result = 'WIN' and pnl < 0)
    or (result = 'LOSS' and pnl > 0)
