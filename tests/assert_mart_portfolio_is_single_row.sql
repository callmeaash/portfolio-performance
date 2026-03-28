-- Fails if the portfolio summart mart has more than 1 row

select count(*) as row_count
from {{ ref('mart_portfolio_summary') }}
having count(*) != 1