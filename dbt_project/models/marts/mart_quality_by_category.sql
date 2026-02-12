with enriched as (
    select * from {{ ref('int_transactions_enriched') }}
)

select
    category,
    count(*) as total,
    sum(case when is_amount_missing then 1 else 0 end) as missing_amounts,
    sum(case when is_amount_suspicious then 1 else 0 end) as suspicious_amounts,
    round(avg(case when amount is not null then amount end), 2) as avg_amount,
    sum(case when is_fraud then 1 else 0 end) as fraud_count
from enriched
where category is not null
group by category