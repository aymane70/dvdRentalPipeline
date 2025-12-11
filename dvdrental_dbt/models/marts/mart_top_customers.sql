{{ config(materialized='table') }}

select
    customer_id,
    customer_name,
    email,
    count(distinct rental_id) as total_rentals,
    sum(payment_amount) as total_spent,
    round(avg(payment_amount),2) as avg_payment
from {{ ref('int_sales_enriched') }}
group by customer_id, customer_name, email
order by total_spent desc
limit 20
