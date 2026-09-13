-- One row per source table: how many rows are currently in the squashed
-- staging view, and the most recent created_at/updated_at seen in it. This
-- is what cdc/api/main.py's /meta endpoint reads to answer "how far behind
-- is the data" -- computed here (against the staging views, which do the
-- bucket rescan) rather than at API-request time, so the API itself never
-- needs S3 access (same reasoning as the rest of ADR-8).
select 'customers' as table_name, count(*) as row_count, max(created_at) as max_created_at, max(updated_at) as max_updated_at from {{ ref('stg_cdc_customers') }}
union all
select 'products', count(*), max(created_at), max(updated_at) from {{ ref('stg_cdc_products') }}
union all
select 'orders', count(*), max(created_at), max(updated_at) from {{ ref('stg_cdc_orders') }}
union all
select 'order_items', count(*), max(created_at), max(updated_at) from {{ ref('stg_cdc_order_items') }}
union all
select 'marketing_spend', count(*), max(created_at), max(updated_at) from {{ ref('stg_cdc_marketing_spend') }}
union all
select 'sessions', count(*), max(created_at), max(updated_at) from {{ ref('stg_cdc_sessions') }}
union all
select 'returns', count(*), max(created_at), max(updated_at) from {{ ref('stg_cdc_returns') }}
union all
select 'inventory', count(*), max(created_at), max(updated_at) from {{ ref('stg_cdc_inventory') }}
