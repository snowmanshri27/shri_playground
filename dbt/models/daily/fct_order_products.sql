{{
    config(
        materialized='view'
    )
}}

select * from {{ ref("stg_order_products") }}
