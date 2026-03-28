-- Fails if any trade has exit date before entry date

select *
from {{ ref('stg_trades') }}
where exit_date < entry_date