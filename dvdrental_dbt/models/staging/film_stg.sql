{{ config(materialized='table') }}

select
    film_id,
    title,
    description,
    release_year,
    language_id,
    rental_duration,
    rental_rate,
    length,
    replacement_cost,
    rating,
    last_update
from
    {{ var('raw_dataset') }}.film_extract


