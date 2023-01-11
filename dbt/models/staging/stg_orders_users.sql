select order_id, user_id, eval_set
from {{ source("instacart_raw_data", "orders") }}
