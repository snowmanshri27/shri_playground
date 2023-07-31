
with
    source as (
        (select * from {{ source("instacart_raw_data", "order_products__prior") }})
        union
        (select * from {{ source("instacart_raw_data", "order_products__train") }})
    ),
    renamed as (select order_id, product_id, add_to_cart_order, reordered from source)
select *
from renamed
