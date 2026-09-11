"""Consumes the Debezium change-event topics and materializes current table
state into a local DuckDB file, so it can be diffed against Postgres (the
actual current state) or the event log (the known history) for CDC
correctness and lag testing -- see cdc/verify.py.

Requires the connector's decimal.handling.mode=double (set in
connector-postgres.json) -- without it, NUMERIC/DECIMAL columns arrive as
base64-encoded bytes rather than plain numbers, since Kafka Connect's
JsonConverter still consults the (implicit) field schema during encoding
even with schemas.enable=false; that flag only controls whether the schema
is *included* in the output, not how values are serialized.

Usage:
    python -m cdc.consumer --idle-exit 15
    python -m cdc.consumer --brokers localhost:29092 --out cdc/materialized.duckdb
"""

import argparse
import datetime
import json
import time

import duckdb
from confluent_kafka import Consumer

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
    table_name VARCHAR,
    op VARCHAR,
    pk_value BIGINT,
    source_commit_ts TIMESTAMP,
    consumed_at TIMESTAMP,
    consumer_lag_seconds DOUBLE
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


def _decode(table: str, col: str, value):
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


def upsert(con: duckdb.DuckDBPyConnection, table: str, after: dict) -> None:
    cols = TABLE_COLUMNS[table]
    row = {c: _decode(table, c, after.get(c)) for c in cols}
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


def log_lag(con: duckdb.DuckDBPyConnection, table: str, op: str, pk_value, source_ts_ms) -> None:
    if source_ts_ms is None:
        return
    source_ts = datetime.datetime.fromtimestamp(source_ts_ms / 1000, tz=datetime.timezone.utc)
    consumed_at = datetime.datetime.now(datetime.timezone.utc)
    lag = (consumed_at - source_ts).total_seconds()
    con.execute(
        "INSERT INTO _cdc_lag_log VALUES (?, ?, ?, ?, ?, ?)",
        [table, op, pk_value, source_ts.replace(tzinfo=None), consumed_at.replace(tzinfo=None), lag],
    )


def run(
    brokers: str,
    group_id: str,
    out_path: str,
    tables: list[str],
    topic_prefix: str,
    from_beginning: bool,
    idle_exit: float | None,
) -> dict:
    con = duckdb.connect(out_path)
    ensure_schema(con)

    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": group_id,
            "auto.offset.reset": "earliest" if from_beginning else "latest",
            "enable.auto.commit": True,
        }
    )
    topics = [f"{topic_prefix}{t}" for t in tables]
    consumer.subscribe(topics)
    print(f"Subscribed to {topics}")

    counts: dict[str, int] = {}
    last_msg_at = time.monotonic()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if idle_exit is not None and (time.monotonic() - last_msg_at) > idle_exit:
                    print(f"No messages for {idle_exit}s, exiting.")
                    break
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            last_msg_at = time.monotonic()
            table = msg.topic().rsplit(".", 1)[-1]
            if table not in TABLE_COLUMNS:
                continue

            raw_value = msg.value()
            if raw_value is None:
                # tombstone following a delete -- already applied when the
                # 'd' event itself was consumed, nothing further to do.
                counts[f"{table}:tombstone"] = counts.get(f"{table}:tombstone", 0) + 1
                continue

            event = json.loads(raw_value)
            op = event.get("op")
            source_ts_ms = (event.get("source") or {}).get("ts_ms")

            if op in ("c", "u", "r") and event.get("after"):
                pk_value = upsert(con, table, event["after"])
            elif op == "d":
                before = event.get("before") or {}
                key = json.loads(msg.key()) if msg.key() else {}
                pk_value = before.get(TABLE_PK[table]) or key.get(TABLE_PK[table])
                if pk_value is not None:
                    delete(con, table, pk_value)
            else:
                continue

            log_lag(con, table, op, pk_value, source_ts_ms)
            counts[f"{table}:{op}"] = counts.get(f"{table}:{op}", 0) + 1
    finally:
        consumer.close()
        con.close()

    return counts


def main():
    parser = argparse.ArgumentParser(description="Materialize Debezium change events into a local DuckDB file.")
    parser.add_argument("--brokers", default="localhost:29092", help="Kafka bootstrap servers (host listener, not the in-network kafka:9092).")
    parser.add_argument("--group-id", default="cdc-materializer")
    parser.add_argument("--out", default="cdc/materialized.duckdb")
    parser.add_argument("--topic-prefix", default="business.public.")
    parser.add_argument("--tables", nargs="+", default=list(TABLE_COLUMNS))
    parser.add_argument("--from-beginning", action="store_true", default=True, help="Consume from the start of each topic (default). Pass --no-from-beginning to resume from latest instead.")
    parser.add_argument("--no-from-beginning", dest="from_beginning", action="store_false")
    parser.add_argument("--idle-exit", type=float, default=None, help="Exit after this many seconds with no new messages (omit to run forever, like a real consumer service).")
    args = parser.parse_args()

    counts = run(
        brokers=args.brokers,
        group_id=args.group_id,
        out_path=args.out,
        tables=args.tables,
        topic_prefix=args.topic_prefix,
        from_beginning=args.from_beginning,
        idle_exit=args.idle_exit,
    )
    print(f"\nMaterialized to {args.out}:")
    for k in sorted(counts):
        print(f"  {k}: {counts[k]}")


if __name__ == "__main__":
    main()
