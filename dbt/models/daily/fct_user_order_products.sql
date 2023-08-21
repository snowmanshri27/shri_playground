
with
    orders as (
        (select * from {{ ref("fct_orders") }})
    ),
    order_products as (
        (select * from {{ ref("fct_order_products") }})
    ),
    daily_order_products as (
        select CAST(row_number() over() AS INT) AS user_order_product_id,
            a.user_id,
            a.order_id,
            b.product_id,
            a.order_number,
            a.order_dow,
            a.order_hour_of_day,
            b.add_to_cart_order,
            b.reordered,
            cast(a.days_since_prior_order AS INT) days_since_prior_order,	
            cast(a.days_since_prior_order_cum AS INT) days_since_prior_order_cum,
            date(a.order_date) as order_date
        from orders a
        join order_products b
        on a.order_id = b.order_id
        order by a.user_id, a.order_id, b.product_id
    )
select *
from daily_order_products
