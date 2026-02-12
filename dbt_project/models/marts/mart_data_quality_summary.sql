with enriched as (
    select * from {{ ref('int_transactions_enriched') }}
)

select
    current_date as report_date,
    count(*) as total_records,
    
    -- Complétude
    sum(case when is_amount_missing then 1 else 0 end) as missing_amounts,
    round(100.0 * sum(case when is_amount_missing then 1 else 0 end) / count(*), 2) as pct_missing_amounts,
    
    sum(case when is_date_missing then 1 else 0 end) as missing_dates,
    round(100.0 * sum(case when is_date_missing then 1 else 0 end) / count(*), 2) as pct_missing_dates,
    
    -- Anomalies
    sum(case when is_amount_suspicious then 1 else 0 end) as suspicious_amounts,
    round(100.0 * sum(case when is_amount_suspicious then 1 else 0 end) / count(*), 2) as pct_suspicious,
    
    -- Fraude
    sum(case when is_fraud then 1 else 0 end) as fraud_count,
    round(100.0 * sum(case when is_fraud then 1 else 0 end) / count(*), 2) as pct_fraud,
    
    -- Score global de qualité (0-100)
    round(100 - (
        100.0 * sum(case when is_amount_missing or is_date_missing or is_amount_suspicious then 1 else 0 end) / count(*)
    ), 2) as data_quality_score
    
from enriched