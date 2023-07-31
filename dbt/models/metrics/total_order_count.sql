-- Ignore this for now; this is just an additional representation of the metric 
--- as a view
select date(order_date) ds, count(1)
from {{ ref('fct_orders') }}
group by 1