"""Batch-loads change events from the s3-sink connector's landed objects in
MinIO/S3 into a separate DuckDB file, incrementally: each run only reads
objects it hasn't already processed (tracked in a small state file), rather
than rescanning and reprocessing everything from scratch every time.

This is the batch counterpart to cdc/consumer.py's live Kafka consumer --
same underlying data (both ultimately trace back to the same
postgres-source connector), same decode logic (cdc/schema_map.py), just a
different path in: object storage discovered on a schedule instead of a
long-running Kafka subscription. The point of having both is to compare
them: cdc/verify.py's row-parity check works the same way against either
DuckDB file, and each writes to its own _cdc_lag_log (tagged loader='batch'
here, 'stream' in consumer.py's) so batch lag -- dominated by the sink's
flush interval *plus* however long between batch runs -- is directly
comparable to streaming lag.

Usage:
    python -m cdc.batch_load                 # process whatever's new, then exit
    python -m cdc.batch_load --loop 300      # repeat every 300s, forever
"""

import argparse
import json
import time

import boto3
import duckdb
from botocore.config import Config as BotoConfig

from . import schema_map

DEFAULT_ENDPOINT = "http://localhost:9000"
DEFAULT_BUCKET = "cdc-events"
DEFAULT_STATE_FILE = "cdc/batch_load_state.json"
DEFAULT_OUT = "cdc/batch_materialized.duckdb"


def _s3_client(endpoint: str, access_key: str, secret_key: str):
    # MinIO needs path-style addressing (bucket in the URL path, not a
    # subdomain) -- boto3's virtual-hosted-style default would try to
    # resolve http://cdc-events.minio:9000, which doesn't exist.
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=BotoConfig(s3={"addressing_style": "path"}),
    )


def load_state(path: str) -> set[str]:
    try:
        with open(path) as f:
            return set(json.load(f).get("processed_keys", []))
    except FileNotFoundError:
        return set()


def save_state(path: str, processed: set[str]) -> None:
    with open(path, "w") as f:
        json.dump({"processed_keys": sorted(processed)}, f, indent=2)


def list_objects(s3, bucket: str) -> list[str]:
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".jsonl"):
                keys.append(obj["Key"])
    return keys


def table_from_key(key: str) -> str | None:
    # business.public.<table>/dt=<yyyy>-<mm>-<dd>/<partition>-<offset>.jsonl
    first_segment = key.split("/", 1)[0]
    table = first_segment.rsplit(".", 1)[-1]
    return table if table in schema_map.TABLE_COLUMNS else None


def apply_line(con: duckdb.DuckDBPyConnection, table: str, line: str, counts: dict) -> None:
    line = line.strip()
    if not line:
        return
    parsed = json.loads(line)
    envelope = parsed.get("value") if isinstance(parsed, dict) else None
    if not envelope:
        # a tombstone -- a null Kafka value renders as {"value": null}
        # (confirmed against live output, not assumed), so `envelope` is
        # None here rather than missing entirely
        counts[f"{table}:tombstone"] = counts.get(f"{table}:tombstone", 0) + 1
        return

    op = envelope.get("op")
    source_ts_ms = (envelope.get("source") or {}).get("ts_ms")

    if op in ("c", "u", "r") and envelope.get("after"):
        pk_value = schema_map.upsert(con, table, envelope["after"])
    elif op == "d":
        before = envelope.get("before") or {}
        pk_value = before.get(schema_map.TABLE_PK[table])
        if pk_value is None:
            return
        schema_map.delete(con, table, pk_value)
    else:
        return

    schema_map.log_lag(con, table, op, pk_value, source_ts_ms, loader="batch")
    counts[f"{table}:{op}"] = counts.get(f"{table}:{op}", 0) + 1


def run_once(con: duckdb.DuckDBPyConnection, s3, bucket: str, state_path: str, processed: set[str]) -> tuple[dict, int]:
    all_keys = list_objects(s3, bucket)
    new_keys = sorted(k for k in all_keys if k not in processed)

    counts: dict[str, int] = {}
    for key in new_keys:
        table = table_from_key(key)
        if table is None:
            continue
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        for line in body.splitlines():
            apply_line(con, table, line, counts)
        # persisted after each object, not once at the end of the run --
        # a crash mid-run then only risks reprocessing the one in-flight
        # object (harmless: upserts are idempotent), not the whole batch
        processed.add(key)
        save_state(state_path, processed)

    return counts, len(new_keys)


def main():
    parser = argparse.ArgumentParser(description="Batch-load new objects from the s3-sink bucket into a DuckDB file, incrementally.")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--access-key", default="minioadmin")
    parser.add_argument("--secret-key", default="minioadmin")
    parser.add_argument("--out", default=DEFAULT_OUT)
    parser.add_argument("--state-file", default=DEFAULT_STATE_FILE)
    parser.add_argument("--loop", type=float, default=None, help="Repeat every N seconds, forever, instead of running once and exiting.")
    args = parser.parse_args()

    s3 = _s3_client(args.endpoint, args.access_key, args.secret_key)
    con = duckdb.connect(args.out)
    schema_map.ensure_schema(con)
    processed = load_state(args.state_file)

    while True:
        counts, n_new_objects = run_once(con, s3, args.bucket, args.state_file, processed)
        if n_new_objects:
            print(f"Processed {n_new_objects} new object(s):")
            for k in sorted(counts):
                print(f"  {k}: {counts[k]}")
        else:
            print("No new objects.")

        if args.loop is None:
            break
        time.sleep(args.loop)

    con.close()


if __name__ == "__main__":
    main()
