-- Daily PnL fact table
-- Calculates realized and unrealized PnL

-- Note: This is a simplified PnL calculation
-- Production systems would use more sophisticated cost basis tracking (FIFO, LIFO, etc.)

with positions as (
    select * from {{ ref('fact_positions_daily') }}
),

prices as (
    select * from {{ ref('int_daily_prices') }}
),

trades as (
    select * from {{ ref('fact_trades') }}
),

-- Calculate average cost basis (simplified)
cost_basis as (
    select
        account_id,
        symbol,
        trade_date,
        sum(case when side = 'BUY' then trade_value else 0 end) as buy_value,
        sum(case when side = 'BUY' then quantity else 0 end) as buy_quantity,
        sum(case when side = 'SELL' then trade_value else 0 end) as sell_value,
        sum(case when side = 'SELL' then quantity else 0 end) as sell_quantity
    from trades
    group by account_id, symbol, trade_date
),

-- Calculate realized PnL (from sales)
realized_pnl as (
    select
        account_id,
        symbol,
        trade_date as pnl_date,
        -- Simplified: assume average cost basis
        sell_value - (sell_quantity * (buy_value / nullif(buy_quantity, 0))) as realized_pnl
    from cost_basis
    where sell_quantity > 0
),

-- Calculate unrealized PnL (mark-to-market)
unrealized_pnl as (
    select
        p.account_id,
        p.symbol,
        p.position_date as pnl_date,
        p.cumulative_position,
        pr.close_price as market_price,
        -- Simplified unrealized PnL calculation
        -- (current market value - cost basis)
        -- For this scaffold, we'll use a placeholder
        p.cumulative_position * pr.close_price as market_value
    from positions p
    inner join prices pr
        on p.symbol = pr.symbol
        and p.position_date = pr.price_date
    where p.cumulative_position != 0
),

-- Combine realized and unrealized
combined_pnl as (
    select
        coalesce(r.account_id, u.account_id) as account_id,
        coalesce(r.pnl_date, u.pnl_date) as pnl_date,
        u.symbol,
        coalesce(r.realized_pnl, 0) as realized_pnl,
        0 as unrealized_pnl,  -- Placeholder: needs proper cost basis tracking
        coalesce(r.realized_pnl, 0) as total_pnl
    from realized_pnl r
    full outer join unrealized_pnl u
        on r.account_id = u.account_id
        and r.symbol = u.symbol
        and r.pnl_date = u.pnl_date
),

final as (
    select
        account_id,
        pnl_date,
        symbol,
        realized_pnl,
        unrealized_pnl,
        total_pnl,
        current_timestamp as calculated_at
    from combined_pnl
)

select * from final
