with transactions as (
    select * from {{ ref('stg_transactions') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

enriched as (
    select
        t.transaction_id,
        t.customer_id,
        c.name as customer_name,
        c.segment as customer_segment,
        t.transaction_date,
        t.amount,
        t.category,
        t.status,
        t.merchant,
        t.country,
        t.is_fraud,
        -- Flags de qualité
        case when t.amount is null then true else false end as is_amount_missing,
        case when t.transaction_date is null then true else false end as is_date_missing,
        case when abs(t.amount) > 10000 then true else false end as is_amount_suspicious
    from transactions t
    left join customers c on t.customer_id = c.customer_id
)

select * from enriched