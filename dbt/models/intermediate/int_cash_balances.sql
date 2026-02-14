-- Intermediate model for cumulative cash balances
-- Calculates running cash balance by account

with cash_events as (
    select
        account_id,
        cash_date,
        cash_type,
        amount
    from {{ ref('stg_cash_events') }}
),

-- Calculate signed amounts (DEPOSIT positive, WITHDRAWAL negative)
signed_cash as (
    select
        account_id,
        cash_date,
        case
            when cash_type in ('DEPOSIT', 'DIVIDEND', 'INTEREST') then amount
            when cash_type = 'WITHDRAWAL' then -amount
        end as signed_amount
    from cash_events
),

-- Aggregate by date
daily_changes as (
    select
        account_id,
        cash_date,
        sum(signed_amount) as daily_change
    from signed_cash
    group by account_id, cash_date
),

-- Calculate cumulative balance
cumulative_balances as (
    select
        account_id,
        cash_date as balance_date,
        daily_change,
        sum(daily_change) over (
            partition by account_id
            order by cash_date
            rows between unbounded preceding and current row
        ) as cash_balance
    from daily_changes
)

select * from cumulative_balances
