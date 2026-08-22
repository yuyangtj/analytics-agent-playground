# Business Database Schema

This DuckDB database contains data for an e-commerce business: customers, the product
catalog, orders and their line items, marketing spend, website sessions, returns, and
warehouse inventory.

## `customers`

One row per customer.

| Column | Type | Notes |
|---|---|---|
| `customer_id` | INTEGER | Primary key. |
| `signup_date` | DATE | Date the customer created their account. |
| `region` | VARCHAR | One of: `US-East`, `US-West`, `EU`, `APAC`, `LATAM`. |
| `acquisition_channel` | VARCHAR | How the customer was acquired. One of: `organic_search`, `paid_search`, `social`, `email`, `referral`, `direct`. May be null. |
| `email_domain` | VARCHAR | Domain portion of the customer's signup email. |

## `products`

One row per product in the catalog.

| Column | Type | Notes |
|---|---|---|
| `product_id` | INTEGER | Primary key. |
| `category` | VARCHAR | Top-level category, e.g. `Electronics`, `Home`, `Apparel`, `Beauty`, `Outdoors`. |
| `subcategory` | VARCHAR | Category-specific subcategory. |
| `cost` | DECIMAL(10,2) | Unit cost to the business. |
| `list_price` | DECIMAL(10,2) | Standard list price. |
| `launch_date` | DATE | Date the product became available for sale. |
| `discontinued_date` | DATE | Date the product was discontinued, if applicable. Null if still active. |

## `orders`

One row per order.

| Column | Type | Notes |
|---|---|---|
| `order_id` | INTEGER | Primary key. |
| `customer_id` | INTEGER | References `customers.customer_id`. |
| `order_date` | DATE | Date the order was placed. |
| `status` | VARCHAR | One of: `completed`, `cancelled`, `refunded`. |
| `channel` | VARCHAR | Purchase channel: `web`, `mobile_app`, or `marketplace`. May be null. |
| `promo_code` | VARCHAR | Promo code applied at checkout, if any. Null if none. |

## `order_items`

One row per line item within an order. An order can have multiple line items.

| Column | Type | Notes |
|---|---|---|
| `order_id` | INTEGER | References `orders.order_id`. |
| `product_id` | INTEGER | References `products.product_id`. |
| `quantity` | INTEGER | Units purchased. |
| `unit_price` | DECIMAL(10,2) | Price per unit at time of purchase. |
| `discount` | DECIMAL(10,2) | Discount amount applied to this line item (not a percentage). |

## `marketing_spend`

Daily marketing spend by channel.

| Column | Type | Notes |
|---|---|---|
| `date` | DATE | Calendar date. |
| `channel` | VARCHAR | Marketing channel, e.g. `paid_search`, `social`, `email`, `referral`. |
| `spend` | DECIMAL(10,2) | Amount spent that day on that channel. |
| `impressions` | INTEGER | Ad impressions served. |
| `clicks` | INTEGER | Ad clicks recorded. |

## `sessions`

Website/app sessions.

| Column | Type | Notes |
|---|---|---|
| `session_id` | INTEGER | Primary key. |
| `customer_id` | INTEGER | References `customers.customer_id`. Null for anonymous/logged-out sessions. |
| `session_date` | DATE | Date of the session. |
| `channel` | VARCHAR | Traffic source for the session. |
| `device` | VARCHAR | One of: `desktop`, `mobile`, `tablet`. |

## `returns`

One row per returned order.

| Column | Type | Notes |
|---|---|---|
| `return_id` | INTEGER | Primary key. |
| `order_id` | INTEGER | References `orders.order_id`. |
| `return_date` | DATE | Date the return was processed. |
| `reason` | VARCHAR | One of: `defective`, `wrong_item`, `no_longer_needed`, `not_as_described`, `other`. |
| `refund_amount` | DECIMAL(10,2) | Amount refunded to the customer. |

## `inventory`

Periodic snapshots of stock levels by product and warehouse.

| Column | Type | Notes |
|---|---|---|
| `product_id` | INTEGER | References `products.product_id`. |
| `warehouse` | VARCHAR | One of: `WH-East`, `WH-West`, `WH-EU`. |
| `snapshot_date` | DATE | Date of the inventory snapshot. |
| `quantity_on_hand` | INTEGER | Units physically in stock at the warehouse. |
| `quantity_reserved` | INTEGER | Units already allocated to open orders. |
| `reorder_point` | INTEGER | Stock level threshold at which reordering is triggered. |

## Relationships

- `orders.customer_id` → `customers.customer_id`
- `order_items.order_id` → `orders.order_id`
- `order_items.product_id` → `products.product_id`
- `sessions.customer_id` → `customers.customer_id`
- `returns.order_id` → `orders.order_id`
- `inventory.product_id` → `products.product_id`

Foreign keys are not enforced at the database level.
