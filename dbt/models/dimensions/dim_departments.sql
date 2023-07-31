{{
    config(
        materialized='view'
    )
}}

with
    source as (select * from {{ source("instacart_raw_data", "departments") }}),
    renamed as (select department_id, department from source)
select *
from renamed
