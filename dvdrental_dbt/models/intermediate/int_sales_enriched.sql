{{ config(materialized='table') }}

with rental_details as (
    select * from {{ ref('int_rental_details') }}
),
film_details as (
    select * from {{ ref('int_film_details') }}
)

select
    r.rental_id,
    r.customer_id,
    r.customer_name,
    r.email,
    r.store_id,
    r.manager_staff_id,
    r.payment_amount,
    r.payment_date,
    r.rental_date,
    r.return_date,
    f.film_id,
    f.title,
    f.category_id,
    f.rental_rate,
    f.length,
    f.rating,
    f.total_copies,
    -- Derived fields
    DATE_DIFF(r.return_date, r.rental_date, DAY) as rental_duration_days,
    case
        when r.payment_amount > f.rental_rate then 'Late Fee Included'
        else 'Normal Payment'
    end as payment_type
from rental_details r
left join film_details f on r.film_id = f.film_id
