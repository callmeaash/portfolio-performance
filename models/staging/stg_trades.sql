{{
    config(
        materialized = 'view',
        description  = 'Cleaned and type-cast trades from raw seed data.'
    )
}}

with source as (
    select * from {{ source('raw', 'trades') }}
),

cleaned as (
    select
        upper(trim(symbol))  as symbol,
        cast(entry_date as date) as entry_date,
        cast(exit_date as date) as exit_date,
        upper(trim(type)) as trade_type,
        cast(entry_price as double) as entry_price,
        cast(exit_price as double) as exit_price,
        cast(pnl as double) as pnl,
        cast(pnl_pct as double) as pnl_pct,
        upper(trim(result)) as result

    from source
    where
        entry_price > 0 and
        exit_price > 0 and
        entry_date is not null and
        exit_date is not null and
        exit_date > entry_date
)

select * from cleaned