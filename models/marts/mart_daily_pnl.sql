{{
  config(
    materialized = 'table',
    description  = 'Daily equity curve with drawdown, rolling returns, and calendar fields.'
  )
}}

with equity as (

    select * from {{ ref('int_daily_equity') }}

)

select
    date,
    daily_pnl,
    trades_closed,
    wins_closed,
    losses_closed,
    round(cumulative_pnl, 2) as cumulative_pnl,
    round(peak_equity, 2) as peak_equity,
    round(drawdown_abs, 2) as drawdown_abs,
    round(drawdown_pct, 2) as drawdown_pct,
    round(rolling_7d_pnl, 2) as rolling_7d_pnl,
    round(rolling_30d_pnl, 2) as rolling_30d_pnl,

    -- convenience flags for dashboards
    cumulative_pnl >= peak_equity as is_new_peak,
    drawdown_abs < 0 as is_in_drawdown,

    -- calendar fields for time-pattern analysis
    date_part('dow', date) as day_of_week,
    date_part('week', date) as week_of_year,
    date_part('month', date) as month_number,
    date_part('year', date) as year_number

from equity
order by date