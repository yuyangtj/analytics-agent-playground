"""Diffs the materialized DuckDB sink (cdc/consumer.py's output) against
live Postgres (the source of truth) and reports consumer lag from the
_cdc_lag_log table it writes alongside the data.

Two things this checks that a plain row-count match wouldn't catch:
  - a row present in Postgres but missing from the sink (an event the
    consumer never saw or hasn't caught up to yet)
  - a row present in the sink but absent from Postgres (a DELETE the
    consumer missed, or -- if Postgres has moved on since the sink was
    read -- just a timing artifact; re-run to check)

Usage:
    python -m cdc.verify
    python -m cdc.verify --db-url postgresql://... --materialized cdc/materialized.duckdb
"""

import argparse
import os
import sys

import duckdb
import psycopg

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

DEFAULT_DB_URL = os.environ.get("CDC_DB_URL", "postgresql://business:business@localhost:5432/business")


def _pg_pk_set(conn: psycopg.Connection, table: str, pk: str) -> set:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {pk} FROM {table}")
        return {row[0] for row in cur.fetchall()}


def _duckdb_pk_set(con: duckdb.DuckDBPyConnection, table: str, pk: str) -> set:
    return {row[0] for row in con.execute(f"SELECT {pk} FROM {table}").fetchall()}


def verify_table(pg_conn: psycopg.Connection, duck_con: duckdb.DuckDBPyConnection, table: str) -> dict:
    pk = TABLE_PK[table]
    pg_pks = _pg_pk_set(pg_conn, table, pk)
    try:
        duck_pks = _duckdb_pk_set(duck_con, table, pk)
    except duckdb.CatalogException:
        duck_pks = set()  # table not created yet -- consumer never saw a message for it

    missing_in_sink = pg_pks - duck_pks  # Postgres has it, sink doesn't yet
    stale_in_sink = duck_pks - pg_pks    # sink has it, Postgres doesn't (missed delete, or a race)

    return {
        "table": table,
        "pg_count": len(pg_pks),
        "sink_count": len(duck_pks),
        "missing_in_sink": sorted(missing_in_sink)[:10],
        "missing_in_sink_count": len(missing_in_sink),
        "stale_in_sink": sorted(stale_in_sink)[:10],
        "stale_in_sink_count": len(stale_in_sink),
        "ok": not missing_in_sink and not stale_in_sink,
    }


def lag_summary(duck_con: duckdb.DuckDBPyConnection) -> list[dict]:
    try:
        rows = duck_con.execute(
            """
            SELECT
                table_name,
                count(*) AS n,
                min(consumer_lag_seconds) AS min_lag,
                quantile_cont(consumer_lag_seconds, 0.5) AS p50_lag,
                quantile_cont(consumer_lag_seconds, 0.95) AS p95_lag,
                max(consumer_lag_seconds) AS max_lag
            FROM _cdc_lag_log
            GROUP BY table_name
            ORDER BY table_name
            """
        ).fetchall()
    except duckdb.CatalogException:
        return []
    cols = ["table_name", "n", "min_lag", "p50_lag", "p95_lag", "max_lag"]
    return [dict(zip(cols, row)) for row in rows]


def main():
    parser = argparse.ArgumentParser(description="Diff the materialized DuckDB sink against Postgres, and report consumer lag.")
    parser.add_argument("--db-url", default=DEFAULT_DB_URL)
    parser.add_argument("--materialized", default="cdc/materialized.duckdb")
    parser.add_argument("--tables", nargs="+", default=list(TABLE_PK))
    args = parser.parse_args()

    pg_conn = psycopg.connect(args.db_url)
    duck_con = duckdb.connect(args.materialized, read_only=True)

    print("=== row parity: Postgres vs materialized sink ===")
    all_ok = True
    for table in args.tables:
        result = verify_table(pg_conn, duck_con, table)
        all_ok &= result["ok"]
        status = "OK" if result["ok"] else "MISMATCH"
        print(f"  [{status}] {table}: pg={result['pg_count']} sink={result['sink_count']}")
        if result["missing_in_sink_count"]:
            print(f"      missing_in_sink ({result['missing_in_sink_count']}): {result['missing_in_sink']}")
        if result["stale_in_sink_count"]:
            print(f"      stale_in_sink ({result['stale_in_sink_count']}): {result['stale_in_sink']}")

    print("\n=== consumer lag (source commit -> consumed), seconds ===")
    lag_rows = lag_summary(duck_con)
    if not lag_rows:
        print("  (no _cdc_lag_log data -- run cdc/consumer.py first)")
    for r in lag_rows:
        print(f"  {r['table_name']}: n={r['n']} min={r['min_lag']:.2f} p50={r['p50_lag']:.2f} p95={r['p95_lag']:.2f} max={r['max_lag']:.2f}")

    pg_conn.close()
    duck_con.close()

    if not all_ok:
        print("\nFAILED: row mismatches found (see above).")
        sys.exit(1)
    print("\nOK: sink matches Postgres for all tables.")


if __name__ == "__main__":
    main()
