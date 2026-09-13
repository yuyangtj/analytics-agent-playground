"""Shared schema/decode logic for anything that materializes Debezium change
events into DuckDB. Factored out of cdc/consumer.py (the streaming/Kafka
path) so cdc/batch_load.py (the batch/object-storage path) doesn't carry a
second, independently-drifting copy of the same table/column/type maps --
exactly the kind of duplication that makes schema drift easy to miss (see
ARCHITECTURE_DECISIONS.md and the schema-registry discussion it followed).

Both consumers apply the *same* Debezium envelope shape, just sourced
differently: cdc/consumer.py reads it live off Kafka topics, cdc/batch_load.py
reads it off objects already landed in MinIO/S3 by the s3-sink connector --
same "after"/"before"/"op"/"source" fields, same DATE/TIMESTAMP encoding,
because both trace back to the same postgres-source connector's output.
"""

import datetime

import duckdb

TABLE_PK = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "marketing_spend": "marketing_spend_id",
    "sessions": "session_id",
    "returns": "return_id",
    "inventory": "inventory_id",
}

TABLE_COLUMNS = {
    "customers": ["customer_id", "signup_date", "region", "acquisition_channel", "email_domain", "created_at", "updated_at"],
    "products": ["product_id", "category", "subcategory", "cost", "list_price", "launch_date", "discontinued_date", "created_at", "updated_at"],
    "orders": ["order_id", "customer_id", "order_date", "status", "channel", "promo_code", "created_at", "updated_at"],
    "order_items": ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount", "created_at", "updated_at"],
    "marketing_spend": ["marketing_spend_id", "spend_date", "channel", "spend", "impressions", "clicks", "created_at", "updated_at"],
    "sessions": ["session_id", "customer_id", "session_date", "channel", "device", "created_at", "updated_at"],
    "returns": ["return_id", "order_id", "return_date", "reason", "refund_amount", "created_at", "updated_at"],
    "inventory": ["inventory_id", "product_id", "warehouse", "snapshot_date", "quantity_on_hand", "quantity_reserved", "reorder_point", "created_at", "updated_at"],
}

# Debezium's default representation: DATE columns -> io.debezium.time.Date
# (integer days since epoch), TIMESTAMPTZ columns -> io.debezium.time.ZonedTimestamp
# (ISO-8601 string, always UTC). Everything else passes through as-is given
# decimal.handling.mode=double.
DATE_COLUMNS = {
    "customers": {"signup_date"},
    "products": {"launch_date", "discontinued_date"},
    "orders": {"order_date"},
    "order_items": set(),
    "marketing_spend": {"spend_date"},
    "sessions": {"session_date"},
    "returns": {"return_date"},
    "inventory": {"snapshot_date"},
}
TIMESTAMP_COLUMNS = {"created_at", "updated_at"}
_EPOCH = datetime.date(1970, 1, 1)

DDL_TEMPLATE = "CREATE TABLE IF NOT EXISTS {table} ({cols}, PRIMARY KEY ({pk}))"
DDL_LAG_LOG = """
CREATE TABLE IF NOT EXISTS _cdc_lag_log (
    loader VARCHAR,          -- 'stream' (cdc/consumer.py) or 'batch' (cdc/batch_load.py)
    table_name VARCHAR,
    op VARCHAR,
    pk_value BIGINT,
    source_commit_ts TIMESTAMP,
    consumed_at TIMESTAMP,
    lag_seconds DOUBLE
)
"""

# DuckDB column types -- kept separate from Postgres's schema.sql since this
# is a read-side materialization, not an OLTP schema (no FKs/triggers needed).
DUCKDB_TYPES = {
    "customer_id": "BIGINT", "product_id": "BIGINT", "order_id": "BIGINT",
    "order_item_id": "BIGINT", "marketing_spend_id": "BIGINT", "session_id": "BIGINT",
    "return_id": "BIGINT", "inventory_id": "BIGINT",
    "signup_date": "DATE", "launch_date": "DATE", "discontinued_date": "DATE",
    "order_date": "DATE", "spend_date": "DATE", "session_date": "DATE",
    "return_date": "DATE", "snapshot_date": "DATE",
    "region": "VARCHAR", "acquisition_channel": "VARCHAR", "email_domain": "VARCHAR",
    "category": "VARCHAR", "subcategory": "VARCHAR", "status": "VARCHAR",
    "channel": "VARCHAR", "promo_code": "VARCHAR", "warehouse": "VARCHAR", "reason": "VARCHAR",
    "device": "VARCHAR",
    "cost": "DOUBLE", "list_price": "DOUBLE", "unit_price": "DOUBLE", "discount": "DOUBLE",
    "spend": "DOUBLE", "refund_amount": "DOUBLE",
    "quantity": "INTEGER", "impressions": "INTEGER", "clicks": "INTEGER",
    "quantity_on_hand": "INTEGER", "quantity_reserved": "INTEGER", "reorder_point": "INTEGER",
    "created_at": "TIMESTAMP", "updated_at": "TIMESTAMP",
}


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    for table, cols in TABLE_COLUMNS.items():
        col_sql = ", ".join(f"{c} {DUCKDB_TYPES[c]}" for c in cols)
        con.execute(DDL_TEMPLATE.format(table=table, cols=col_sql, pk=TABLE_PK[table]))
    con.execute(DDL_LAG_LOG)


def decode(table: str, col: str, value):
    if value is None:
        return None
    if col in DATE_COLUMNS.get(table, ()):
        return _EPOCH + datetime.timedelta(days=value)
    if col in TIMESTAMP_COLUMNS:
        # Debezium's ZonedTimestamp is always UTC; store naive-UTC rather
        # than tz-aware -- duckdb's Python binding silently converts an
        # aware datetime to the *local system* timezone before storing it
        # as a naive TIMESTAMP, which would quietly skew every lag
        # calculation by the host's UTC offset otherwise.
        dt = datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return value


def upsert(con: duckdb.DuckDBPyConnection, table: str, after: dict):
    cols = TABLE_COLUMNS[table]
    row = {c: decode(table, c, after.get(c)) for c in cols}
    pk_col = TABLE_PK[table]
    col_sql = ", ".join(cols)
    placeholders = ", ".join(["?"] * len(cols))
    update_sql = ", ".join(f"{c} = excluded.{c}" for c in cols if c != pk_col)
    con.execute(
        f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT ({pk_col}) DO UPDATE SET {update_sql}",
        [row[c] for c in cols],
    )
    return row[pk_col]


def delete(con: duckdb.DuckDBPyConnection, table: str, pk_value) -> None:
    con.execute(f"DELETE FROM {table} WHERE {TABLE_PK[table]} = ?", [pk_value])


def log_lag(con: duckdb.DuckDBPyConnection, table: str, op: str, pk_value, source_ts_ms, loader: str = "stream") -> None:
    if source_ts_ms is None:
        return
    source_ts = datetime.datetime.fromtimestamp(source_ts_ms / 1000, tz=datetime.timezone.utc)
    consumed_at = datetime.datetime.now(datetime.timezone.utc)
    lag = (consumed_at - source_ts).total_seconds()
    con.execute(
        "INSERT INTO _cdc_lag_log VALUES (?, ?, ?, ?, ?, ?, ?)",
        [loader, table, op, pk_value, source_ts.replace(tzinfo=None), consumed_at.replace(tzinfo=None), lag],
    )
