{{ config(materialized='table') }}

select
    customer_id,
    store_id,
    concat(first_name, ' ', last_name) as full_name,
    email,
    address_id,
    SAFE_CAST(activebool AS BOOL) AS active,
    create_date,
    last_update
from
    {{ var('raw_dataset') }}.customer_extract

