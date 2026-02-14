-- Symbol dimension table
-- Contains unique trading symbols

with trade_symbols as (
    select distinct symbol
    from {{ ref('stg_trades') }}
),

price_symbols as (
    select distinct symbol
    from {{ ref('stg_prices') }}
),

all_symbols as (
    select symbol from trade_symbols
    union
    select symbol from price_symbols
),

final as (
    select
        symbol,
        -- Add symbol metadata here if available
        -- (company name, sector, etc.)
        current_timestamp as created_at
    from all_symbols
)

select * from final
