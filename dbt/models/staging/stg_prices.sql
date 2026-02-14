-- Staging model for price events
-- Cleans and standardizes price data

with source as (
    select * from {{ source('raw', 'raw_prices') }}
),

cleaned as (
    select
        symbol,
        price,
        timestamp as price_timestamp,
        cast(timestamp as date) as price_date,
        coalesce(source, 'unknown') as source,
        event_version,
        ingestion_time
    from source
    where timestamp is not null
      and symbol is not null
      and price > 0
)

select * from cleaned
