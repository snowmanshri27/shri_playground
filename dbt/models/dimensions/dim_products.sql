{{
    config(
        materialized='view'
    )
}}

with
    source as (select * from {{ source("instacart_raw_data", "products") }}),
    renamed as (select product_id, product_name, aisle_id, department_id from source)
select *
from renamed
