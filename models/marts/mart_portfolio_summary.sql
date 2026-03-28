{{
    config(
        materialized = 'table',
        description = 'Portfolio summary with KPI values'
    )
}}

with trades as (

    select * from {{ ref('int_trade_metrics') }}
),

equity as (

    select * from {{ ref('int_daily_equity') }}
),

trade_stats as (

    select
        count(*) as total_trades,
        sum(is_win) as total_wins,
        sum(is_loss) as total_losses,

        -- win rate
        round(avg(is_win) * 100, 2) as win_rate_pct,

        -- pnl aggregrates
        round(sum(pnl), 2) as total_pnl,
        round(avg(pnl_pct), 2) as avg_pnl_pct,
        round(max(pnl), 2) as best_trade_pnl,
        round(min(pnl), 2) as worst_trade_pnl,
        round(max(pnl_pct), 2) as best_trade_pnl_pct,
        round(min(pnl_pct), 2) as worst_trade_pnl_pct,

        -- profit factor
        round(sum(gross_profit), 2) as gross_profit,
        round(sum(gross_loss), 2 ) as gross_loss,
        round(
            sum(gross_profit) / nullif(abs(sum(gross_loss)), 0), 2
        ) as profit_factor,

        -- average win / loss
        round(avg(win_pct), 2) as avg_win_pct,
        round(avg(loss_pct), 2) as avg_loss_pct,

        -- expectancy
        round(
            (avg(is_win) * avg(win_pct)) - 
            (avg(is_loss) * avg(loss_pct)), 2
        ) as expectancy_pct,

        -- holding days
        round(avg(holding_days), 1) as avg_holding_days,
        max(holding_days) as max_holding_days,
        min(holding_days) as min_holding_days,

        -- coverage
        count(distinct symbol) as symbols_traded,
        min(entry_date) as first_trade_date,
        max(exit_date) as last_trade_date,

        -- trade type split
        count(*) filter (where trade_type = 'BUY') as total_buy_trades,
        count(*) filter (where trade_type = 'SELL') as total_sell_trades,

        -- duration bucket distribution
        count(*) filter (where trade_duration_bucket = 'INTRADAY') as intraday_trades,
        count(*) filter (where trade_duration_bucket = 'SWING') as swing_trades,
        count(*) filter (where trade_duration_bucket = 'SHORT_TERM') as short_term_trades,
        count(*) filter (where trade_duration_bucket = 'LONG_TERM') as long_term_trades
    
    from trades
),

drawdown_stats as (

    select
        round(max(drawdown_abs), 2) as max_drawdown_abs,
        round(max(drawdown_pct), 2) as max_drawdown_pct,
        round(max(peak_equity), 2) as peak_equity
    
    from equity
)

select 
    ts.*,
    ds.*,

    round(
        ts.total_pnl / nullif(ds.max_drawdown_abs, 0), 2
    ) as calmer_ratio

from trade_stats as ts
cross join drawdown_stats as ds