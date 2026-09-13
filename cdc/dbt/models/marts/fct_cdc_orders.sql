-- Order-level fact: one row per order, with item-level revenue rolled up.
-- LEFT JOINs throughout (not INNER) so an order with no items yet, or a
-- dangling product_id, shows up as a NULL/zero rather than silently
-- disappearing -- same principle as dbt/models/marts/fct_orders.sql in the
-- benchmark arm, applied here to the CDC-squashed current state instead of
-- a DuckDB snapshot.
with item_totals as (
    select
        order_id,
        sum(quantity * (unit_price - discount)) as order_revenue,
        sum(quantity) as total_quantity,
        count(*) as line_item_count
    from {{ ref('stg_cdc_order_items') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    o.channel,
    o.promo_code,
    c.region as customer_region,
    c.acquisition_channel as customer_acquisition_channel,
    coalesce(it.order_revenue, 0) as order_revenue,
    coalesce(it.total_quantity, 0) as total_quantity,
    coalesce(it.line_item_count, 0) as line_item_count
from {{ ref('stg_cdc_orders') }} as o
left join item_totals as it on o.order_id = it.order_id
left join {{ ref('stg_cdc_customers') }} as c on o.customer_id = c.customer_id
