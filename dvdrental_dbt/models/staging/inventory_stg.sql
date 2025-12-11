{{ config(materialized='table') }}

select
    inventory_id,
    film_id,
    store_id,
    last_update
from
    {{ var('raw_dataset') }}.inventory_extract

