-- Rolling volatility metric
-- Calculates 30-day rolling standard deviation of returns

with daily_returns as (
    select
        account_id,
        value_date,
        daily_return
    from {{ ref('daily_returns') }}
),

-- Calculate 30-day rolling volatility
rolling_vol as (
    select
        account_id,
        value_date,
        daily_return,
        stddev(daily_return) over (
            partition by account_id
            order by value_date
            rows between 29 preceding and current row
        ) as volatility_30d
    from daily_returns
),

-- Annualize volatility
annualized as (
    select
        account_id,
        value_date,
        volatility_30d,
        -- Annualize using square root of trading days
        volatility_30d * sqrt({{ var('trading_days_per_year') }}) as annualized_volatility
    from rolling_vol
    where volatility_30d is not null
),

final as (
    select
        account_id,
        value_date,
        volatility_30d,
        volatility_30d * 100 as volatility_30d_pct,
        annualized_volatility,
        annualized_volatility * 100 as annualized_volatility_pct,
        current_timestamp as calculated_at
    from annualized
)

select * from final
