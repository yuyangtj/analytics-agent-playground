select
    session_id,
    customer_id,
    session_date,
    channel,
    device
from {{ source('raw', 'sessions') }}
