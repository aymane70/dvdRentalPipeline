{{ config(materialized='table') }}

select
    f.category_id,
    count(distinct s.rental_id) as total_rentals,
    sum(s.payment_amount) as total_revenue,
    avg(s.rental_duration_days) as avg_duration
from {{ ref('int_sales_enriched') }} s
left join {{ ref('int_film_details') }} f on s.film_id = f.film_id
group by f.category_id
order by total_revenue desc
