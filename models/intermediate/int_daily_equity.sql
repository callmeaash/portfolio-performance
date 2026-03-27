{{
    config(
        materialized = 'ephemeral',
        description = 'Daily trade metrics'
    )
}}

with daily_pnl as (

    select
        exit_date as date,
        sum(pnl) as daily_pnl,
        count(*) as trades_closed,
        sum(is_win) as wins_closed,
        sum(is_loss) as losses_closed

    from {{ ref('int_trade_metrics') }}
    group by exit_date
),

with_cumulative_pnl as (

    select
        *,

        sum(daily_pnl) over (
            order by date
            rows between unbounded preceding and current row
        ) as cumulative_pnl,

    from daily_pnl
),

with_peak_equity as (

    select
        *,

        max(cumulative_pnl) over (
            order by date
            rows between unbounded preceding and current row
        ) as peak_equity
    
    from with_cumulative_pnl
),

with_drawdown as (

    select
        *,

        round(cumulative_pnl - peak_equity, 2) as drawdown,
        round(cumulative_pnl - peak_equity, 2) / nullif(peak_equity, 0) * 100 as drawdown_pct,

        sum(daily_pnl) over (
            order by date
            rows between 6 preceding and current row
        ) as rolling_7d_pnl,

        sum(daily_pnl) over (
            order by date
            rows between 29 preceding and current row
        ) as rolling_30d_pnl
    
    from with_peak_equity
)

select * from with_drawdown

