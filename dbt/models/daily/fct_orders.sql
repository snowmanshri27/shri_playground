{{
    config(
        materialized='view'
    )
}}

select * from {{ ref("stg_orders") }}
