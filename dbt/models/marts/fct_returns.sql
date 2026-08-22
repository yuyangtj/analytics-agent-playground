select
    return_id,
    order_id,
    return_date,
    reason,
    refund_amount
from {{ ref('stg_returns') }}
