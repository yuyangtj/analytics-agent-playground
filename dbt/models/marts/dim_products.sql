select
    product_id,
    category,
    subcategory,
    cost,
    list_price,
    launch_date,
    discontinued_date
from {{ ref('stg_products') }}
