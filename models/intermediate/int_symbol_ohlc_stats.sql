{{
    config(
        materialized = 'ephemeral',
        description = 'Per-symbol summary statistics derived from OHLC data.'
    )
}}

with ohlc as (
    
    select * from {{ ref('stg_ohlc') }}
),

stats as (

    select
        symbol,

        round(min(low), 2) as all_time_low,
        round(max(high), 2) as all_time_high,
        round(avg(close), 2) as avg_close,
        round(stddev(close), 2) as stddev_close,

        round(avg(daily_range_pct), 2) as avg_daily_range_pct,

        round(avg(volume), 0) as avg_daily_volume,
        round(sum(volume), 0) as total_volume,
        round(sum(traded_amount), 2) as average_daily_amount,

        round(avg(rsi_14), 2) as avg_rsi,
        round(avg(macd), 4) as avg_macd,

        count(*) filter (
            where macd_trend = 'BULLISH'
        ) as bullish_days,

        count(*) filter (
            where macd_trend = 'BEARISH'
        ) as bearish_days,

        count(*) filter (
            where rsi_zone = 'OVERBOUGHT'
        ) as overbought_days,

        count(*) filter (
            where rsi_zone = 'OVERSOLD'
        ) as oversold_days,

        count(*) as total_trading_days,
        min(date) as first_trading_day,
        max(date) as last_trading_day

    from ohlc
    group by symbol
)

select
    *,
    round(bullish_days  * 100.0 / nullif(total_trading_days, 0), 1) as bullish_day_pct,
    round(bearish_days  * 100.0 / nullif(total_trading_days, 0), 1) as bearish_day_pct,
    round(overbought_days * 100.0 / nullif(total_trading_days, 0), 1) as overbought_day_pct,
    round(oversold_days   * 100.0 / nullif(total_trading_days, 0), 1) as oversold_day_pct

from stats