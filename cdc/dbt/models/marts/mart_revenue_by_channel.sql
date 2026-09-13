-- Daily revenue and order counts by order channel, completed orders only.
select
    order_date,
    channel,
    count(*) as order_count,
    sum(order_revenue) as total_revenue,
    round(avg(order_revenue), 2) as avg_order_value
from {{ ref('fct_cdc_orders') }}
where status = 'completed'
group by order_date, channel
order by order_date, channel
