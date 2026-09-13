-- Current stock position per product+warehouse, straight off the
-- CDC-squashed current state (stg_cdc_inventory is already "latest row per
-- inventory_id", so this mart just adds the low-stock flag on top).
select
    product_id,
    warehouse,
    quantity_on_hand,
    quantity_reserved,
    quantity_on_hand - quantity_reserved as quantity_available,
    reorder_point,
    quantity_on_hand <= reorder_point as needs_reorder,
    snapshot_date as as_of_date
from {{ ref('stg_cdc_inventory') }}
