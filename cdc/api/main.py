"""A small analytics API serving the metrics computed by cdc/dbt/'s marts,
meant to sit behind a frontend dashboard -- this is the "someone else reads
the raw storage later, on their own schedule" idea from ARCHITECTURE_DECISIONS.md
ADR-1, taken one step further: not just reading the data, but serving it.

Reads cdc/dbt/cdc_raw.duckdb directly, read-only. Doesn't touch Kafka,
Postgres, or MinIO/S3 itself -- `./cdc/dbt/run.sh` (a separate, periodic
batch step; see that file for why it's a wrapper and not a bare `dbt run`)
is what refreshes the mart *tables* this API queries; the API's own job is
just fast, read-only serving of whatever's already there. Marts are
materialized as `table`, not `view` (see cdc/dbt/dbt_project.yml), so a
request here is a plain DuckDB table read, not a full bucket rescan.

Also serves the static frontend (cdc/frontend/) at /app -- same origin as
the API, deliberately, so the frontend's fetch() calls need no CORS
configuration at all: one process, one URL, no cross-origin surface.

Run it:
    .venv/bin/python -m cdc.api.main
    # or: .venv/bin/uvicorn cdc.api.main:app --reload

Then see the dashboard at http://localhost:8000/app/, or the
auto-generated API docs at http://localhost:8000/docs.

DuckDB file-locking note (same caveat as dbt/README.md's for the benchmark
arm): don't run `dbt run` against cdc/dbt/cdc_raw.duckdb at the same moment
this API is serving a request against it -- a write connection and
concurrent access don't mix. Each endpoint opens its own short-lived
read-only connection rather than holding one open for the app's lifetime,
which minimizes (but doesn't eliminate) that window.
"""

import os
from typing import Optional

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles

DB_PATH = os.environ.get("CDC_ANALYTICS_DB", os.path.join(os.path.dirname(__file__), "..", "dbt", "cdc_raw.duckdb"))
FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend")

app = FastAPI(
    title="CDC Analytics API",
    description="Serves metrics computed by cdc/dbt/'s marts, for a frontend dashboard to call.",
    version="0.1.0",
)


def _query(sql: str, params: Optional[list] = None) -> list[dict]:
    """Opens a fresh read-only connection, runs one query, returns rows as
    a list of dicts. Not a shared/global connection -- see module docstring
    for why (DuckDB single-writer locking)."""
    if not os.path.exists(DB_PATH):
        raise HTTPException(
            status_code=503,
            detail=f"{DB_PATH} doesn't exist yet -- run `./cdc/dbt/run.sh` first.",
        )
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
    except duckdb.IOException as e:
        # most likely: dbt run is writing to this file right now
        raise HTTPException(status_code=503, detail=f"Database temporarily unavailable (likely a concurrent `dbt run`): {e}")
    try:
        cur = con.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    except duckdb.CatalogException as e:
        raise HTTPException(status_code=503, detail=f"Mart table not found -- run `./cdc/dbt/run.sh` first: {e}")
    finally:
        con.close()


@app.get("/health")
def health():
    """Whether the marts database exists and its tables are queryable --
    not whether the CDC pipeline itself is up (this API never touches
    Kafka/Postgres/MinIO directly)."""
    if not os.path.exists(DB_PATH):
        return {"status": "not_ready", "detail": "cdc_raw.duckdb doesn't exist yet -- run `./cdc/dbt/run.sh`."}
    try:
        rows = _query("select table_name from information_schema.tables where table_schema = 'main_marts' order by 1")
        return {"status": "ok", "mart_tables": [r["table_name"] for r in rows]}
    except HTTPException as e:
        return {"status": "error", "detail": e.detail}


@app.get("/metrics/revenue-by-channel")
def revenue_by_channel(
    channel: Optional[str] = None,
    start_date: Optional[str] = Query(None, description="Inclusive, YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="Inclusive, YYYY-MM-DD"),
):
    sql = "select * from main_marts.mart_revenue_by_channel where 1=1"
    params = []
    if channel:
        sql += " and channel = ?"
        params.append(channel)
    if start_date:
        sql += " and order_date >= ?"
        params.append(start_date)
    if end_date:
        sql += " and order_date <= ?"
        params.append(end_date)
    sql += " order by order_date, channel"
    return _query(sql, params)


@app.get("/metrics/customers-by-region")
def customers_by_region(region: Optional[str] = None):
    sql = "select * from main_marts.mart_customers_by_region where 1=1"
    params = []
    if region:
        sql += " and region = ?"
        params.append(region)
    sql += " order by region, acquisition_channel"
    return _query(sql, params)


@app.get("/metrics/inventory")
def inventory(
    warehouse: Optional[str] = None,
    needs_reorder: Optional[bool] = None,
):
    sql = "select * from main_marts.mart_inventory_current where 1=1"
    params = []
    if warehouse:
        sql += " and warehouse = ?"
        params.append(warehouse)
    if needs_reorder is not None:
        sql += " and needs_reorder = ?"
        params.append(needs_reorder)
    sql += " order by product_id, warehouse"
    return _query(sql, params)


@app.get("/orders")
def orders(
    status: Optional[str] = None,
    channel: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    sql = "select * from main_marts.fct_cdc_orders where 1=1"
    params = []
    if status:
        sql += " and status = ?"
        params.append(status)
    if channel:
        sql += " and channel = ?"
        params.append(channel)
    sql += " order by order_date, order_id limit ? offset ?"
    params += [limit, offset]
    return _query(sql, params)


@app.get("/orders/{order_id}")
def order_detail(order_id: int):
    rows = _query("select * from main_marts.fct_cdc_orders where order_id = ?", [order_id])
    if not rows:
        raise HTTPException(status_code=404, detail=f"No order {order_id}")
    return rows[0]


# Mounted last, deliberately -- after every API route above, so /app never
# shadows an API path. html=True serves cdc/frontend/index.html for /app/
# and any sub-path that doesn't match a real file (needed for client-side
# routing if this ever grows beyond one page).
if os.path.isdir(FRONTEND_DIR):
    app.mount("/app", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
