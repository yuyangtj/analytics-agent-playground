select
    customer_id,
    signup_date,
    region,
    acquisition_channel,
    email_domain
from {{ source('raw', 'customers') }}
