{{
    config(
        materialized = 'table',
        description  = 'Monthly aggregated performance: PnL, win rate, best/worst symbol.'
    )
}}

with trades as (

    select * from {{ ref('int_trade_metrics') }}
),

monthly as (

    select
        exit_year as year,
        exit_quarter as quarter,
        exit_month as month,

        count(*) as total_trades,
        sum(is_win) as wins,
        sum(is_Loss) as losses,
        round(avg(is_win) * 100, 2) as win_rate_pct,

        round(sum(pnl), 2) as total_pnl,
        round(avg(pnl_pct), 2) as avg_pnl_pct,
        round(max(pnl), 2) as best_trade_pnl,
        round(min(pnl), 2) as worst_trade_pnl,

        round(sum(gross_profit), 2) as gross_profit,
        round(sum(gross_loss), 2) as gross_loss,
        round(
            sum(gross_profit) / nullif(abs(sum(gross_loss)), 0), 2
        ) as profit_factor,

        count(*) filter (where trade_type = 'BUY') as buy_trades,
        count(*) filter (where trade_type = 'SELL') as sell_trades,

        count(distinct symbol) as symbols_traded

    from trades
    group by exit_year, exit_quarter, exit_month
),

with_rank as (

    select
        *,

        rank() over (order by total_pnl desc) as pnl_rank,

        total_pnl - lag(total_pnl) over (
            order by month
        ) as monthly_pnl_change,

        sum(total_pnl) over (
            order by month
            rows between unbounded preceding and current row
        ) as cumulative_pnl,

        case when total_pnl >= 0 then 'GREEN' else 'RED' end as month_result

    from monthly
)

select * from with_rank
order by month