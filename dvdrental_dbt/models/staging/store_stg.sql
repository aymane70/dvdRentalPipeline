{{ config(materialized='table') }}

select
    store_id,
    manager_staff_id,
    last_update
from
    {{ var('raw_dataset') }}.store_extract


