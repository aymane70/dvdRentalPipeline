{{ config(materialized='table') }}

select
    rental_id,
    rental_date,
    inventory_id,
    customer_id,
    return_date,
    last_update
from
    {{ var('raw_dataset') }}.rental_extract

