select
    date as spend_date,
    channel,
    spend,
    impressions,
    clicks
from {{ source('raw', 'marketing_spend') }}
