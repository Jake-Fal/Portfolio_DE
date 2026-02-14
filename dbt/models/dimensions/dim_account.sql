-- Account dimension table
-- Contains unique accounts from trades and cash events

with trade_accounts as (
    select distinct account_id
    from {{ ref('stg_trades') }}
),

cash_accounts as (
    select distinct account_id
    from {{ ref('stg_cash_events') }}
),

all_accounts as (
    select account_id from trade_accounts
    union
    select account_id from cash_accounts
),

final as (
    select
        account_id,
        -- Add account metadata here if available
        current_timestamp as created_at
    from all_accounts
)

select * from final
