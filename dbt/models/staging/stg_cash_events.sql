-- Staging model for cash events
-- Standardizes cash flow data

with source as (
    select * from {{ source('raw', 'raw_cash_events') }}
),

cleaned as (
    select
        event_id,
        account_id,
        amount,
        upper(type) as cash_type,
        timestamp as cash_timestamp,
        cast(timestamp as date) as cash_date,
        description,
        event_version,
        ingestion_time
    from source
    where timestamp is not null
      and event_id is not null
      and amount > 0
)

select * from cleaned
