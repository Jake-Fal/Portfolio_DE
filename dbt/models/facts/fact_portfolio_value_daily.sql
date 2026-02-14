-- Daily portfolio value fact table
-- Combines positions, prices, and cash to calculate total portfolio value

with positions as (
    select * from {{ ref('fact_positions_daily') }}
),

prices as (
    select * from {{ ref('int_daily_prices') }}
),

cash_balances as (
    select * from {{ ref('int_cash_balances') }}
),

-- Join positions with prices to get market value
position_values as (
    select
        p.account_id,
        p.symbol,
        p.position_date,
        p.cumulative_position,
        pr.close_price,
        p.cumulative_position * pr.close_price as position_value
    from positions p
    inner join prices pr
        on p.symbol = pr.symbol
        and p.position_date = pr.price_date
    where p.cumulative_position != 0  -- Filter out closed positions
),

-- Aggregate position values by account and date
account_position_values as (
    select
        account_id,
        position_date as value_date,
        sum(position_value) as total_position_value
    from position_values
    group by account_id, position_date
),

-- Get cash balances
account_cash as (
    select
        account_id,
        balance_date as value_date,
        cash_balance
    from cash_balances
),

-- Combine position values and cash
portfolio_value as (
    select
        coalesce(pv.account_id, cb.account_id) as account_id,
        coalesce(pv.value_date, cb.value_date) as value_date,
        coalesce(pv.total_position_value, 0) as positions_value,
        coalesce(cb.cash_balance, 0) as cash_balance,
        coalesce(pv.total_position_value, 0) + coalesce(cb.cash_balance, 0) as total_value
    from account_position_values pv
    full outer join account_cash cb
        on pv.account_id = cb.account_id
        and pv.value_date = cb.value_date
),

final as (
    select
        account_id,
        value_date,
        positions_value,
        cash_balance,
        total_value,
        current_timestamp as calculated_at
    from portfolio_value
)

select * from final
