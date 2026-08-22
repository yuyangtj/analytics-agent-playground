select
    order_id,
    customer_id,
    order_date,
    status,
    channel,
    promo_code
from {{ source('raw', 'orders') }}
