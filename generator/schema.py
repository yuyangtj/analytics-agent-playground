"""DuckDB table DDL for the synthetic business."""

DDL_STATEMENTS = [
    """
    CREATE TABLE customers (
        customer_id INTEGER PRIMARY KEY,
        signup_date DATE NOT NULL,
        region VARCHAR NOT NULL,
        acquisition_channel VARCHAR,
        email_domain VARCHAR NOT NULL
    )
    """,
    """
    CREATE TABLE products (
        product_id INTEGER PRIMARY KEY,
        category VARCHAR NOT NULL,
        subcategory VARCHAR NOT NULL,
        cost DECIMAL(10,2) NOT NULL,
        list_price DECIMAL(10,2) NOT NULL,
        launch_date DATE NOT NULL,
        discontinued_date DATE
    )
    """,
    """
    CREATE TABLE orders (
        order_id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        order_date DATE NOT NULL,
        status VARCHAR NOT NULL,
        channel VARCHAR,
        promo_code VARCHAR
    )
    """,
    """
    CREATE TABLE order_items (
        order_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price DECIMAL(10,2) NOT NULL,
        discount DECIMAL(10,2) NOT NULL DEFAULT 0
    )
    """,
    """
    CREATE TABLE marketing_spend (
        date DATE NOT NULL,
        channel VARCHAR NOT NULL,
        spend DECIMAL(10,2) NOT NULL,
        impressions INTEGER NOT NULL,
        clicks INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE sessions (
        session_id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        session_date DATE NOT NULL,
        channel VARCHAR NOT NULL,
        device VARCHAR NOT NULL
    )
    """,
    """
    CREATE TABLE returns (
        return_id INTEGER PRIMARY KEY,
        order_id INTEGER NOT NULL,
        return_date DATE NOT NULL,
        reason VARCHAR NOT NULL,
        refund_amount DECIMAL(10,2) NOT NULL
    )
    """,
    """
    CREATE TABLE inventory (
        product_id INTEGER NOT NULL,
        warehouse VARCHAR NOT NULL,
        snapshot_date DATE NOT NULL,
        quantity_on_hand INTEGER NOT NULL,
        quantity_reserved INTEGER NOT NULL,
        reorder_point INTEGER NOT NULL
    )
    """,
]


def create_schema(con):
    for ddl in DDL_STATEMENTS:
        con.execute(ddl)
