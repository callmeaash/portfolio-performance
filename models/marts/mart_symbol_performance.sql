{{
    config(
        materialized = 'table',
        description = 'Per-symbol performance metrices ranked by total pnl'
    )
}}

with trades as (

    select * from {{ ref('int_trade_metrics') }}
),

ohlc_stats as (
    
    select * from {{ ref('int_symbol_ohlc_stats') }}
),

symbol_trades as (

    select
        symbol,

        count(*) as total_trades,
        sum(is_win) as wins,
        sum(is_loss) as losses,
        round(avg(is_win) * 100, 2) as win_rate_pct,

        -- pnl
        round(sum(pnl), 2) as total_pnl,
        round(avg(pnl_pct), 2) as avg_pnl_pct,
        round(max(pnl), 2) as best_trade_pnl,
        round(min(pnl), 2) as worst_trade_pnl,

        -- profit factor
        round(sum(gross_profit), 2) as gross_profit,
        round(sum(gross_loss),   2) as gross_loss,
        round(
            sum(gross_profit) / nullif(abs(sum(gross_loss)), 0), 2
        ) as profit_factor,

        -- avg win / loss
        round(avg(win_pct),  2) as avg_win_pct,
        round(avg(loss_pct), 2) as avg_loss_pct,

        -- expectancy
        round(
            (avg(is_win)  * avg(win_pct))
            - (avg(is_loss) * avg(loss_pct)), 2
        ) as expectancy_pct,

        -- holding
        round(avg(holding_days), 1) as avg_holding_days,

        -- trade type split
        count(*) filter (where trade_type = 'BUY') as buy_trades,
        count(*) filter (where trade_type = 'SELL') as sell_trades,

        -- dates
        min(entry_date) as first_trade_date,
        max(exit_date) as last_trade_date

    from trades
    group by symbol
),

ranked as (
    
    select
        st.*,
        os.all_time_low,
        os.all_time_high,
        os.avg_close,
        os.avg_daily_range_pct as volatility_pct,
        os.avg_daily_volume,
        os.avg_rsi,
        os.bullish_day_pct,
        os.bearish_day_pct,
        
        -- portfolio contribution rank
        rank() over (order by st.total_pnl desc) as pnl_rank,
        rank() over (order by st.win_rate_pct desc) as win_rate_rank,
        rank() over (order by st.profit_factor desc) as profit_factor_rank,

        -- performance tier
        case
            when st.total_pnl > 0 and st.win_rate_pct >= 50 then 'STAR'
            when st.total_pnl > 0 and st.win_rate_pct <  50 then 'PROFITABLE'
            when st.total_pnl < 0 and st.win_rate_pct >= 50 then 'UNDERPERFORMER'
            else 'AVOID'
        end as performance_tier

    from symbol_trades st
    left join ohlc_stats os using (symbol)
)

select * from ranked
order by pnl_rank