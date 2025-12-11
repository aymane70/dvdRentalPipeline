{{ config(materialized='table') }}

select
    film_id,
    category_id,
    last_update
from
    {{ var('raw_dataset') }}.film_category_extract

