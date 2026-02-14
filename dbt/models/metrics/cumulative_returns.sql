-- Cumulative returns metric
-- Calculates cumulative return from inception for each account

with daily_returns as (
    select
        account_id,
        value_date,
        daily_return
    from {{ ref('daily_returns') }}
),

-- Calculate cumulative return as product of (1 + daily_return)
cumulative as (
    select
        account_id,
        value_date,
        daily_return,
        exp(sum(ln(1 + daily_return)) over (
            partition by account_id
            order by value_date
            rows between unbounded preceding and current row
        )) - 1 as cumulative_return
    from daily_returns
    where daily_return is not null
),

final as (
    select
        account_id,
        value_date,
        cumulative_return,
        cumulative_return * 100 as cumulative_return_pct,
        current_timestamp as calculated_at
    from cumulative
)

select * from final
