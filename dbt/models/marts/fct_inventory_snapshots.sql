select
    product_id,
    warehouse,
    snapshot_date,
    quantity_on_hand,
    quantity_reserved,
    reorder_point
from {{ ref('stg_inventory') }}
