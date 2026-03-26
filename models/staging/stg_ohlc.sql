{{
    config(
        materialized = 'view',
        description  = 'Cleaned OHLC and technical indicator data from raw seed.'
    )
}}

with source as (
    select * from {{ source('raw', 'ohlc')}}
),

cleaned as (
    select
        cast(date as date) as date,
        upper(trim(symbol)) as symbol,
        cast(open as double) as open,
        cast(high as double) as high,
        cast(low as double) as low,
        cast(close as double) as close,
        cast(volume as double) as volume,
        cast(amount as double) as traded_amount,
        cast(per_change as double) as per_change,
        cast(MACD as double) as macd,
        cast(MACD_signal as double) as macd_signal,
        cast(RSI_14 as double) as rsi_14,
        
        round(cast(high as double) - cast(low as double), 2) as daily_range,
        round(
            (cast(high as double) - cast(low as double))
            / nullif(cast(low as double), 0) * 100, 2
        ) as daily_range_pct,
        
        case
            when cast(MACD as double) > cast(MACD_signal as double) then 'BULLISH'
            else 'BEARISH'
        end as macd_trend,

        case
            when cast(RSI_14 as double) >= 70 then 'OVERBOUGHT'
            when cast(RSI_14 as double) <= 30 then 'OVERSOLD'
            else 'NEUTRAL'
        end as rsi_zone
        
    from source
    where
        close > 0
        and date is not null
)

select * from cleaned

