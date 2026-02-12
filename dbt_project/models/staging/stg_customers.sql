with source as (
    select * from raw.customers
)

select
    customer_id,
    name,
    lower(trim(email)) as email,
    cast(registration_date as date) as registration_date,
    lower(segment) as segment,
    upper(country) as country
from source