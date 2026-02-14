-- Fact table for all trades
-- Enriched with dimension keys and calculated fields

with trades as (
    select * from {{ ref('stg_trades') }}
),

final as (
    select
        trade_id,
        account_id,
        symbol,
        side,
        quantity,
        price,
        -- Calculate trade value
        quantity * price as trade_value,
        trade_timestamp,
        trade_date,
        event_version
    from trades
)

select * from final
