{{ config(materialized='table') }}

select
    format_date('%Y-%m', payment_date) as month,
    sum(payment_amount) as total_revenue,
    count(distinct rental_id) as total_rentals
from {{ ref('int_sales_enriched') }}
group by month
order by month
