-- Postgres OLTP schema for the CDC pipeline.
--
-- Mirrors generator/schema.py's DuckDB tables, with the changes an OLTP source
-- needs for CDC to work correctly:
--   * every table gets a real single-column PK (Debezium/logical replication
--     needs a stable row identity to emit correct UPDATE/DELETE events;
--     order_items/marketing_spend/inventory had none in the DuckDB version)
--   * FKs are enforced here (the DuckDB analytical copy deliberately leaves
--     orphans in place post-injection; this OLTP copy stays clean --
--     orphan scenarios for CDC testing should come from explicit
--     delete-without-cascade events in the replay stream, not a broken schema)
--   * created_at/updated_at on every table, for measuring source-to-sink lag
--     independent of whatever metadata Debezium itself attaches
--   * REPLICA IDENTITY FULL everywhere for now, so UPDATE/DELETE events carry
--     full before-images -- simplest correct default; can be narrowed to
--     REPLICA IDENTITY DEFAULT (PK-only) per table once the pipeline is
--     working, to see how that changes what's recoverable downstream

CREATE TABLE customers (
    customer_id BIGINT PRIMARY KEY,
    signup_date DATE NOT NULL,
    region VARCHAR NOT NULL,
    acquisition_channel VARCHAR,
    email_domain VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
    product_id BIGINT PRIMARY KEY,
    category VARCHAR NOT NULL,
    subcategory VARCHAR NOT NULL,
    cost DECIMAL(10,2) NOT NULL,
    list_price DECIMAL(10,2) NOT NULL,
    launch_date DATE NOT NULL,
    discontinued_date DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customers(customer_id),
    order_date DATE NOT NULL,
    status VARCHAR NOT NULL,
    channel VARCHAR,
    promo_code VARCHAR,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_item_id BIGINT PRIMARY KEY,  -- surrogate: DuckDB version has no PK
    order_id BIGINT NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id BIGINT NOT NULL REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount DECIMAL(10,2) NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE marketing_spend (
    marketing_spend_id BIGINT PRIMARY KEY,  -- surrogate: DuckDB version keys on (date, channel)
    spend_date DATE NOT NULL,
    channel VARCHAR NOT NULL,
    spend DECIMAL(10,2) NOT NULL,
    impressions INTEGER NOT NULL,
    clicks INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (spend_date, channel)
);

CREATE TABLE sessions (
    session_id BIGINT PRIMARY KEY,
    customer_id BIGINT REFERENCES customers(customer_id),
    session_date DATE NOT NULL,
    channel VARCHAR NOT NULL,
    device VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE returns (
    return_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders(order_id),
    return_date DATE NOT NULL,
    reason VARCHAR NOT NULL,
    refund_amount DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE inventory (
    inventory_id BIGINT PRIMARY KEY,  -- surrogate: DuckDB version keys on (product_id, warehouse, snapshot_date)
    product_id BIGINT NOT NULL REFERENCES products(product_id),
    warehouse VARCHAR NOT NULL,
    snapshot_date DATE NOT NULL,
    quantity_on_hand INTEGER NOT NULL,
    quantity_reserved INTEGER NOT NULL,
    reorder_point INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (product_id, warehouse, snapshot_date)
);

-- Keep updated_at honest on every UPDATE, independent of whatever the replay
-- script sets explicitly -- gives a source-side timestamp to diff against the
-- CDC event's own timestamp for lag measurement.
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    t TEXT;
BEGIN
    FOR t IN SELECT unnest(ARRAY[
        'customers', 'products', 'orders', 'order_items',
        'marketing_spend', 'sessions', 'returns', 'inventory'
    ])
    LOOP
        EXECUTE format(
            'CREATE TRIGGER trg_set_updated_at BEFORE UPDATE ON %I
             FOR EACH ROW EXECUTE FUNCTION set_updated_at()', t
        );
        EXECUTE format('ALTER TABLE %I REPLICA IDENTITY FULL', t);
    END LOOP;
END $$;

-- Debezium's Postgres connector needs a replication role and a publication
-- covering the tables it should stream. wal_level=logical must be set in
-- postgresql.conf (or the container's command args) -- it can't be set here.
CREATE ROLE cdc_replication WITH REPLICATION LOGIN PASSWORD 'cdc_replication';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_replication;

CREATE PUBLICATION cdc_publication FOR TABLE
    customers, products, orders, order_items,
    marketing_spend, sessions, returns, inventory;
