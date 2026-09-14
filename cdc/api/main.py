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

import datetime as dt
import os
import pathlib
from typing import Optional

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles

DB_PATH = os.environ.get("CDC_ANALYTICS_DB", os.path.join(os.path.dirname(__file__), "..", "dbt", "cdc_raw.duckdb"))
FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend")
DBT_DIR = pathlib.Path(os.path.dirname(__file__)) / ".." / "dbt"

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


@app.get("/meta")
def meta():
    """When the marts were last refreshed, and how far behind the
    underlying *business data* is relative to now -- two different things,
    easy to conflate:

    - `marts_refreshed_at`/`_ago_seconds`: when `dbt run` last wrote
      cdc_raw.duckdb (this file's own mtime -- if dbt hasn't run in an
      hour, these numbers are an hour stale even if the pipeline upstream
      is perfectly healthy).
    - `data_as_of`/`data_lag_seconds`: the latest `updated_at` seen across
      all 8 tables in `mart_data_freshness` (itself computed against the
      staging views at `dbt run` time, not queried live here -- this
      endpoint never touches MinIO/S3, same as every other one). This is
      end-to-end lag: Postgres commit -> Debezium -> Kafka -> s3-sink's
      flush interval -> whenever `dbt run` last happened to run, all
      folded into one number.
    """
    result: dict = {}

    if os.path.exists(DB_PATH):
        refreshed_at = dt.datetime.utcfromtimestamp(os.path.getmtime(DB_PATH))
        result["marts_refreshed_at"] = refreshed_at.isoformat() + "Z"
        result["marts_refreshed_ago_seconds"] = (dt.datetime.utcnow() - refreshed_at).total_seconds()
    else:
        result["marts_refreshed_at"] = None
        result["marts_refreshed_ago_seconds"] = None

    freshness_rows = _query("select * from main_marts.mart_data_freshness order by table_name")
    now = dt.datetime.utcnow()
    max_updated_at = None
    tables = []
    for r in freshness_rows:
        updated = r.get("max_updated_at")
        lag = (now - updated).total_seconds() if updated is not None else None
        tables.append({**r, "lag_seconds": lag})
        if updated is not None and (max_updated_at is None or updated > max_updated_at):
            max_updated_at = updated

    result["tables"] = tables
    result["data_as_of"] = (max_updated_at.isoformat() + "Z") if max_updated_at else None
    result["data_lag_seconds"] = (now - max_updated_at).total_seconds() if max_updated_at else None
    return result


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


def _metricflow_query(metrics: list[str], group_by: list[str], where: Optional[str]) -> dict:
    """Builds a fresh MetricFlowEngine, runs one query, discards it.

    Deliberately NOT a persistent/global engine, even though building one
    costs ~0.9s (vs ~10-20ms per query on an already-built one) -- confirmed
    live (see ARCHITECTURE_DECISIONS.md ADR-12) that a held engine keeps its
    DuckDB connection open indefinitely, which then flat-out blocks (lock
    conflict, not just staleness) any concurrent `./cdc/dbt/run.sh`. A
    per-request engine mirrors `_query()`'s fresh-connection-per-request
    philosophy (ADR-8) and keeps the same short, self-closing window open
    against cdc_raw.duckdb.

    CLIConfiguration resolves its DuckDB file path relative to the process
    CWD, not the project-dir argument (the same footgun cdc/dbt/run.sh
    exists to work around for the dbt CLI) -- so this chdir's into cdc/dbt/
    for the duration of the call and restores CWD in a `finally`, rather
    than relying on the API process happening to be launched from there.

    Also explicitly tears down the dbt-duckdb adapter's connection before
    returning -- confirmed live that skipping this breaks every *other*
    endpoint too, for the rest of the process's life: `adapter.connections
    .cleanup_all()` alone isn't enough, because `DuckDBConnectionManager
    ._ENV` (the object actually holding the raw duckdb connection) is a
    *class* attribute, shared by every adapter instance in the process --
    closing this request's Connection wrapper doesn't clear it. The
    surviving `_ENV` then makes every later plain `_query()` read-only
    `duckdb.connect()` fail with "Can't open a connection to same database
    file with a different configuration than existing connections", even
    though nothing here holds a live Python reference to that engine or
    its connection anymore. `close_all_connections()` is the classmethod
    that actually clears `_ENV`.
    """
    from dbt_metricflow.cli.cli_configuration import CLIConfiguration
    from metricflow.engine.metricflow_engine import MetricFlowQueryRequest

    if not DBT_DIR.exists():
        raise HTTPException(status_code=503, detail=f"{DBT_DIR} doesn't exist.")

    original_cwd = os.getcwd()
    os.chdir(DBT_DIR)
    cfg = None
    try:
        cfg = CLIConfiguration()
        cfg.setup(dbt_profiles_path=pathlib.Path("."), dbt_project_path=pathlib.Path("."), configure_file_logging=False)
        req = MetricFlowQueryRequest.create(
            metric_names=metrics,
            group_by_names=group_by,
            where_constraints=[where] if where else None,
        )
        result = cfg.mf.query(req)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        if cfg is not None:
            connections = cfg.dbt_artifacts.adapter.connections
            connections.cleanup_all()
            type(connections).close_all_connections()
        os.chdir(original_cwd)

    df = result.result_df
    return {"columns": list(df.column_names), "rows": [list(row) for row in df.rows]}


@app.get("/semantic/query")
def semantic_query(
    metrics: str = Query(..., description="Comma-separated metric names, e.g. total_revenue,order_count"),
    group_by: str = Query("", description="Comma-separated dimension names, e.g. order_id__channel"),
    where: Optional[str] = Query(None, description="MetricFlow where-clause filter, e.g. \"{{ Dimension('order_id__channel') }} = 'web'\""),
):
    """Same underlying marts as /metrics/*, but served through the dbt
    Semantic Layer (MetricFlow) instead of hand-written SQL -- see
    cdc/SEMANTIC_LAYER.md for why both exist side by side. Metric and
    dimension names come from cdc/dbt/models/marts/_semantic.yml; the
    dimension naming convention (`order_id__channel`, not just `channel`)
    is MetricFlow's, not this API's."""
    metric_names = [m.strip() for m in metrics.split(",") if m.strip()]
    group_by_names = [g.strip() for g in group_by.split(",") if g.strip()]
    if not metric_names:
        raise HTTPException(status_code=400, detail="`metrics` must name at least one metric.")
    return _metricflow_query(metric_names, group_by_names, where)


# Mounted last, deliberately -- after every API route above, so /app never
# shadows an API path. html=True serves cdc/frontend/index.html for /app/
# and any sub-path that doesn't match a real file (needed for client-side
# routing if this ever grows beyond one page).
if os.path.isdir(FRONTEND_DIR):
    app.mount("/app", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
