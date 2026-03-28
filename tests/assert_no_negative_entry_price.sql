-- Fails if there are any trades with a negative entry price


select *
from {{ ref('stg_trades') }}
where entry_price < 0