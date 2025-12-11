{{ config(materialized='table') }}

with film as (
    select * from {{ ref('film_stg') }}
),
film_category as (
    select * from {{ ref('film_category_stg') }}
),
inventory as (
    select * from {{ ref('inventory_stg') }}
)

select
    f.film_id,
    f.title,
    f.description,
    f.release_year,
    f.language_id,
    f.rental_duration,
    f.rental_rate,
    f.length,
    f.replacement_cost,
    f.rating,
    fc.category_id,
    i.store_id,
    count(i.inventory_id) as total_copies
from film f
left join film_category fc on f.film_id = fc.film_id
left join inventory i on f.film_id = i.film_id
group by
    f.film_id, f.title, f.description, f.release_year, f.language_id,
    f.rental_duration, f.rental_rate, f.length, f.replacement_cost, f.rating,
    fc.category_id, i.store_id
