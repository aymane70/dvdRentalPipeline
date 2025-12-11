{{ config(materialized='table') }}

select
    film_id,
    title,
    category_id,
    count(distinct rental_id) as total_rentals,
    sum(payment_amount) as total_revenue,
    avg(rental_duration_days) as avg_rental_duration
from {{ ref('int_sales_enriched') }}
group by film_id, title, category_id
order by total_revenue desc
limit 20
