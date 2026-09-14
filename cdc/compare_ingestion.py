"""Reports lag side by side across the three ingestion strategies this
pipeline already runs against the same underlying event stream:

  - streaming  -- cdc/consumer.py, a live Kafka consumer
  - micro-batch -- cdc/batch_load.py, incremental, on-demand, object-key tracked
  - pure batch  -- cdc/dbt/, full bucket rescan on every `dbt run`, no
                   incremental state at all

This doesn't run any of the three itself -- it reads whatever each one has
already produced (see cdc/INGESTION_STRATEGIES.md for how to populate all
three first) and prints one consolidated comparison. Streaming and
micro-batch both log lag per-event (a real distribution, min/p50/p95/max);
pure batch doesn't -- dbt has no concept of "this one row's lag," only
"how stale is the newest row as of the last full run" (cdc/api/'s /meta
endpoint, read here over HTTP rather than reimplemented) -- which is
itself one of the findings, not a limitation of this script: batch-style
ingestion structurally gives you a point-in-time freshness number, not a
per-event lag distribution.

Usage:
    python -m cdc.compare_ingestion
    python -m cdc.compare_ingestion --api-url http://localhost:8000
"""

import argparse
import os

import duckdb
import urllib.request
import json

STREAM_DB = os.path.join(os.path.dirname(__file__), "materialized.duckdb")
BATCH_DB = os.path.join(os.path.dirname(__file__), "batch_materialized.duckdb")


def _lag_stats(db_path: str, loader: str) -> dict | None:
    if not os.path.exists(db_path):
        return None
    con = duckdb.connect(db_path, read_only=True)
    try:
        row = con.execute(
            """
            select
                count(*) as n,
                min(lag_seconds) as min_lag,
                quantile_cont(lag_seconds, 0.5) as p50_lag,
                quantile_cont(lag_seconds, 0.95) as p95_lag,
                max(lag_seconds) as max_lag
            from _cdc_lag_log
            where loader = ?
            """,
            [loader],
        ).fetchone()
    except duckdb.CatalogException:
        return None
    finally:
        con.close()
    if row is None or row[0] == 0:
        return None
    return {"n": row[0], "min": row[1], "p50": row[2], "p95": row[3], "max": row[4]}


def _batch_freshness(api_url: str) -> dict | None:
    try:
        with urllib.request.urlopen(f"{api_url}/meta", timeout=5) as resp:
            return json.load(resp)
    except Exception as e:
        print(f"  (couldn't reach {api_url}/meta: {e})")
        return None


def _fmt(seconds: float | None) -> str:
    if seconds is None:
        return "n/a"
    return f"{seconds:.1f}s"


def main():
    parser = argparse.ArgumentParser(description="Compare lag across streaming, micro-batch, and pure-batch ingestion.")
    parser.add_argument("--api-url", default="http://localhost:8000")
    args = parser.parse_args()

    print("=== Streaming (cdc/consumer.py -> materialized.duckdb) ===")
    stream = _lag_stats(STREAM_DB, "stream")
    if stream:
        print(f"  n={stream['n']}  min={_fmt(stream['min'])}  p50={_fmt(stream['p50'])}  p95={_fmt(stream['p95'])}  max={_fmt(stream['max'])}")
    else:
        print("  no data -- run: .venv/bin/python -m cdc.consumer --idle-exit 20")

    print("\n=== Micro-batch (cdc/batch_load.py -> batch_materialized.duckdb) ===")
    batch = _lag_stats(BATCH_DB, "batch")
    if batch:
        print(f"  n={batch['n']}  min={_fmt(batch['min'])}  p50={_fmt(batch['p50'])}  p95={_fmt(batch['p95'])}  max={_fmt(batch['max'])}")
    else:
        print("  no data -- run: .venv/bin/python -m cdc.batch_load")

    print(f"\n=== Pure batch (cdc/dbt/ -> {args.api_url}/meta) ===")
    freshness = _batch_freshness(args.api_url)
    if freshness and freshness.get("data_lag_seconds") is not None:
        print(f"  data_as_of={freshness['data_as_of']}  lag={_fmt(freshness['data_lag_seconds'])}  (point-in-time only, not a distribution -- see module docstring)")
        print(f"  marts refreshed {_fmt(freshness['marts_refreshed_ago_seconds'])} ago")
    else:
        print("  no data -- run cdc/dbt/run.sh, then start cdc/api/ (python -m cdc.api.main)")

    print("\n=== What this actually shows ===")
    print("  Streaming and micro-batch both log lag per-event, so p50/p95/max are")
    print("  real distributions over every row captured. Pure batch (dbt) has no")
    print("  per-row lag concept at all -- only 'how stale is the newest row as of")
    print("  the last full run,' a single point-in-time number. That structural")
    print("  difference -- not just the numeric gap -- is the actual comparison.")


if __name__ == "__main__":
    main()
