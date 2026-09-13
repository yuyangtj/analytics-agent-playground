"""Consumes the Debezium change-event topics and materializes current table
state into a local DuckDB file, so it can be diffed against Postgres (the
actual current state) or the event log (the known history) for CDC
correctness and lag testing -- see cdc/verify.py.

This is the *streaming* path: a long-running Kafka consumer applying each
change as it arrives. cdc/batch_load.py is the batch counterpart, reading
the same underlying data off objects already landed by the s3-sink
connector instead of live off Kafka -- both share cdc/schema_map.py's
table/column/type maps and decode logic, so there's exactly one place that
knows Debezium's envelope shape, not two independently-drifting copies.

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
import json
import time

import duckdb
from confluent_kafka import Consumer

from . import schema_map


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
    schema_map.ensure_schema(con)

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
            if table not in schema_map.TABLE_COLUMNS:
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
                pk_value = schema_map.upsert(con, table, event["after"])
            elif op == "d":
                before = event.get("before") or {}
                key = json.loads(msg.key()) if msg.key() else {}
                pk_value = before.get(schema_map.TABLE_PK[table]) or key.get(schema_map.TABLE_PK[table])
                if pk_value is not None:
                    schema_map.delete(con, table, pk_value)
            else:
                continue

            schema_map.log_lag(con, table, op, pk_value, source_ts_ms)
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
    parser.add_argument("--tables", nargs="+", default=list(schema_map.TABLE_COLUMNS))
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
