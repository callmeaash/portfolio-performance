{{
    config(
        materialized = 'ephemeral',
        description = 'Trade records enriched with derived metrics used across the marts.'
    )
}}

with trades as (
    select * from {{ ref('stg_trades') }}
),

enriched as (
    select
        symbol,
        entry_date,
        exit_date,
        trade_type,
        entry_price,
        exit_price,
        pnl,
        pnl_pct,
        result,

        cast(exit_date - entry_date as integer) as holding_days,

        case when result = 'BUY' then 1 else 0 end as is_win,
        case when result = 'LOSS' then 1 else 0 end as is_loss,

        case when pnl > 0 then pnl else 0 end as gross_profit,
        case when pnl < 0 then pnl else 0 end as gross_loss,

        case when pnl_pct > 0 then pnl_pct else null end as win_pct,
        case when pnl_pct < 0 then abs(pnl_pct) else null end as loss_pct,

        case 
            when cast(exit_date - entry_date as integer) = 0 then 'INTRADAY'
            when cast(exit_date - entry_date as integer) <= 5 then 'SWING'
            when cast(exit_date - entry_date as integer) <= 30 then 'SHORT_TERM'
            else 'LONG_TERM'
        end as trade_duration_bucket,

        date_part('month', exit_date) as exit_month,
        date_part('year', exit_date) as exit_year,
        date_part('quarter', exit_date) as exit_quarter,

    from trades
)

select * from enriched