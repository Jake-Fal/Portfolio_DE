-- Drawdown metric
-- Calculates drawdown from peak portfolio value

with portfolio_value as (
    select
        account_id,
        value_date,
        total_value
    from {{ ref('fact_portfolio_value_daily') }}
),

-- Calculate running maximum (peak value)
with_peak as (
    select
        account_id,
        value_date,
        total_value,
        max(total_value) over (
            partition by account_id
            order by value_date
            rows between unbounded preceding and current row
        ) as peak_value
    from portfolio_value
),

-- Calculate drawdown
drawdown_calc as (
    select
        account_id,
        value_date,
        total_value,
        peak_value,
        case
            when peak_value > 0 then
                (total_value - peak_value) / peak_value
            else 0
        end as drawdown
    from with_peak
),

-- Calculate max drawdown
with_max_drawdown as (
    select
        account_id,
        value_date,
        drawdown,
        min(drawdown) over (
            partition by account_id
            order by value_date
            rows between unbounded preceding and current row
        ) as max_drawdown
    from drawdown_calc
),

final as (
    select
        account_id,
        value_date,
        drawdown,
        drawdown * 100 as drawdown_pct,
        max_drawdown,
        max_drawdown * 100 as max_drawdown_pct,
        current_timestamp as calculated_at
    from with_max_drawdown
)

select * from final
