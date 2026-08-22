-- Grain: one row per order line item (order_items).
-- LEFT JOINs throughout (not INNER) so orphaned references - a product_id or
-- customer_id that doesn't exist - surface as NULLs here rather than being
-- silently dropped. No status filtering: every order status is included.
select
    oi.order_id,
    oi.product_id,
    oi.quantity,
    oi.unit_price,
    oi.discount,
    oi.quantity * (oi.unit_price - oi.discount) as line_revenue,
    o.customer_id,
    o.order_date,
    o.status as order_status,
    o.channel as order_channel,
    o.promo_code,
    c.region as customer_region,
    c.acquisition_channel as customer_acquisition_channel,
    p.category as product_category,
    p.subcategory as product_subcategory
from {{ ref('stg_order_items') }} oi
left join {{ ref('stg_orders') }} o on oi.order_id = o.order_id
left join {{ ref('stg_customers') }} c on o.customer_id = c.customer_id
left join {{ ref('stg_products') }} p on oi.product_id = p.product_id
