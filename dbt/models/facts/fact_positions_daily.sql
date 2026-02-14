-- Daily position fact table
-- Calculates cumulative positions by account and symbol

with trades as (
    select
        account_id,
        symbol,
        trade_date,
        side,
        quantity
    from {{ ref('stg_trades') }}
),

-- Calculate signed quantity (BUY positive, SELL negative)
signed_trades as (
    select
        account_id,
        symbol,
        trade_date,
        case
            when side = 'BUY' then quantity
            when side = 'SELL' then -quantity
        end as signed_quantity
    from trades
),

-- Aggregate by date
daily_changes as (
    select
        account_id,
        symbol,
        trade_date,
        sum(signed_quantity) as daily_change
    from signed_trades
    group by account_id, symbol, trade_date
),

-- Calculate cumulative positions
cumulative_positions as (
    select
        account_id,
        symbol,
        trade_date as position_date,
        daily_change,
        sum(daily_change) over (
            partition by account_id, symbol
            order by trade_date
            rows between unbounded preceding and current row
        ) as cumulative_position
    from daily_changes
),

final as (
    select
        account_id,
        symbol,
        position_date,
        daily_change,
        cumulative_position,
        current_timestamp as calculated_at
    from cumulative_positions
)

select * from final
