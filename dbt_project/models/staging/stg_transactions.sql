with source as (
    select * from raw.transactions
),

cleaned as (
    select
        transaction_id,
        customer_id,
        cast(transaction_date as timestamp) as transaction_date,
        cast(amount as decimal(12,2)) as amount,
        lower(trim(category)) as category,
        lower(trim(status)) as status,
        merchant,
        upper(country) as country,
        is_fraud
    from source
    where transaction_id is not null
)

select * from cleaned