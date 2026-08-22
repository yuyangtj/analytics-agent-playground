select
    spend_date,
    channel,
    spend,
    impressions,
    clicks
from {{ ref('stg_marketing_spend') }}
