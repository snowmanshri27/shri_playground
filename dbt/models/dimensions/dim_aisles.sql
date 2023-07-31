{{
    config(
        materialized='view'
    )
}}

with
    source as (select * from {{ source("instacart_raw_data", "aisles") }}),
    renamed as (select aisle_id, aisle from source)
select *
from renamed
