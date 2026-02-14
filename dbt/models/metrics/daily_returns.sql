-- Daily returns metric
-- Calculates percentage change in portfolio value day-over-day

with portfolio_value as (
    select
        account_id,
        value_date,
        total_value
    from {{ ref('fact_portfolio_value_daily') }}
),

-- Get previous day's value
lagged_value as (
    select
        account_id,
        value_date,
        total_value as current_value,
        lag(total_value) over (
            partition by account_id
            order by value_date
        ) as previous_value
    from portfolio_value
),

-- Calculate daily return
returns as (
    select
        account_id,
        value_date,
        current_value,
        previous_value,
        case
            when previous_value > 0 then
                (current_value - previous_value) / previous_value
            else null
        end as daily_return
    from lagged_value
    where previous_value is not null
),

final as (
    select
        account_id,
        value_date,
        daily_return,
        daily_return * 100 as daily_return_pct,
        current_timestamp as calculated_at
    from returns
)

select * from final
