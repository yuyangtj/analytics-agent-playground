select
    region,
    acquisition_channel,
    count(*) as customer_count
from {{ ref('stg_cdc_customers') }}
group by region, acquisition_channel
order by region, acquisition_channel
