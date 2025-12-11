{{ config(materialized='table') }}

select
    payment_id,
    customer_id,
    rental_id,
    amount,
    payment_date
  
from
    {{ var('raw_dataset') }}.payment_extract

