-- Intermediate model for daily closing prices
-- Takes the last price of each day for each symbol

with prices as (
    select
        symbol,
        price,
        price_timestamp,
        price_date
    from {{ ref('stg_prices') }}
),

-- Get last price per symbol per day
daily_close as (
    select
        symbol,
        price_date,
        price as close_price
    from (
        select
            symbol,
            price_date,
            price,
            row_number() over (
                partition by symbol, price_date
                order by price_timestamp desc
            ) as rn
        from prices
    ) ranked
    where rn = 1
)

select * from daily_close
