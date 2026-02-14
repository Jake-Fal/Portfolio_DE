-- Staging model for trade events
-- Applies basic cleaning and standardization

with source as (
    select * from {{ source('raw', 'raw_trades') }}
),

cleaned as (
    select
        trade_id,
        account_id,
        symbol,
        upper(side) as side,
        quantity,
        price,
        timestamp as trade_timestamp,
        cast(timestamp as date) as trade_date,
        event_version,
        ingestion_time
    from source
    where timestamp is not null
      and trade_id is not null
)

select * from cleaned
