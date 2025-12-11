{{ config(materialized='table') }}

select
    store_id,
    count(distinct rental_id) as total_rentals,
    sum(payment_amount) as total_revenue,
    avg(payment_amount) as avg_revenue_per_rental
from {{ ref('int_sales_enriched') }}
group by store_id
order by total_revenue desc
