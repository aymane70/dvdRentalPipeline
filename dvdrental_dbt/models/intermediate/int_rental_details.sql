{{ config(materialized='table') }}

with rental as (
    select * from {{ ref('rental_stg') }}
),
customer as (
    select * from {{ ref('customer_stg') }}
),
store as (
    select * from {{ ref('store_stg') }}
),
payment as (
    select * from {{ ref('payment_stg') }}
),
inventory as (
    select * from {{ ref('inventory_stg') }}
)

select
    r.rental_id,
    r.rental_date,
    r.return_date,
    c.customer_id,
    c.full_name as customer_name,
    c.email,
    c.active,
    s.store_id,
    s.manager_staff_id,
    p.amount as payment_amount,
    p.payment_date,
    i.film_id
from rental r
left join customer c on r.customer_id = c.customer_id
left join payment p on r.rental_id = p.rental_id
left join inventory i on r.inventory_id = i.inventory_id
left join store s on i.store_id = s.store_id
