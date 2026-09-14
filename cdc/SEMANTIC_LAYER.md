# Serving layer: hand-written SQL vs. the dbt Semantic Layer

Two ways of serving the same marts to a caller, both live in `cdc/api/main.py`
against the exact same `cdc_raw.duckdb` — reframed here as a deliberate
comparison, the way `INGESTION_STRATEGIES.md` compares the three ways
of getting data *into* that file. Neither replaces the other; the question
is what each buys you and what it costs.

## The two serving layers

| | REST API (`/metrics/*`, `/orders*`) | Semantic layer (`/semantic/query`) |
|---|---|---|
| Metric/dimension logic lives in | Hand-written SQL per endpoint, in Python | `cdc/dbt/models/marts/_semantic.yml`, declared once |
| Adding a new metric | Write a new SQL query + endpoint | Add one `measures:` entry, reuse the existing endpoint |
| Adding a new cut of an existing metric | Add a `WHERE`/`GROUP BY` to that endpoint's SQL | Just pass a different `group_by`/`where` — no code change |
| Query shape a caller can ask for | Whatever each endpoint was written to accept | Any metric × any declared dimension combination |
| What runs underneath | One `duckdb.connect(..., read_only=True)` per request | A `MetricFlowEngine` compiling the request into SQL, per request |
| Per-request cost | A few ms (thin SQL over an already-materialized table) | ~0.9s to build the engine + ~10-20ms to run the query |
| Failure mode on a bad request | Whatever DuckDB says about the SQL | MetricFlow's own validation — e.g. an unknown metric name comes back with a ranked list of valid suggestions |

## Why both exist, not just one

The REST API is the obvious choice if you know in advance exactly which
questions callers will ask — it's cheap, and the SQL is right there to
read. It stops being cheap the moment someone wants a cut that wasn't
anticipated: revenue by channel *and* region *and* month means either a
new endpoint per combination, or a generic query-builder endpoint that
ends up re-deriving a chunk of what a semantic layer already does (valid
dimension names, join logic, aggregation rules) — just without the
validation.

The semantic layer's cost is the inverse: the ~0.9s engine-build means
it will never be as cheap per-request as the SQL endpoints for a *known*
question, but every metric × dimension combination declared in
`_semantic.yml` is answerable without touching Python. It also gives
better errors for free — see the suggestions list below.

## Verified live

Same underlying mart (`fct_cdc_orders`), two paths, same answer:

```
$ mf query --metrics total_revenue --group-by order_id__channel
| order_id__channel   |   total_revenue |
|:--------------------|-----------------:|
| marketplace         |         16220.95 |
| web                 |         13008.91 |
| mobile_app          |         11449.20 |

$ curl 'localhost:8000/semantic/query?metrics=total_revenue&group_by=order_id__channel'
{"columns":["order_id__channel","total_revenue"],
 "rows":[["marketplace",16220.95],["web",13008.91],["mobile_app",11449.20]]}
```

matching an independently-computed Postgres query on the same figures.
Also checked: an unknown metric name doesn't 500 — it comes back with
MetricFlow's own suggestion list:

```
$ curl 'localhost:8000/semantic/query?metrics=not_a_real_metric'
{"detail":"...The given input does not exactly match any known metrics.
Suggestions:\n      ['total_revenue', 'order_count'] ..."}
```

## The operational question this raised: hold the engine, or rebuild it?

`_query()` (the REST API's helper) already made this call for plain SQL:
open a fresh read-only connection per request rather than holding one
open for the app's lifetime, because DuckDB is single-writer and a held
connection would block `./cdc/dbt/run.sh` indefinitely (ADR-8). MetricFlow
raises the same question with a much bigger number on the "just hold it"
side of the ledger — ~0.9s to build an engine vs. ~10-20ms per query once
built, a 90x difference that makes holding it look obviously worth doing.

Tested directly rather than assumed (full writeup: ADR-12). A background
process built one `MetricFlowEngine`, held it, and a `dbt run` was fired
while it was still alive. The `dbt run` didn't just see stale data — it
failed outright with a DuckDB lock conflict, because the held engine's
connection is exclusive for as long as the process holding it lives. So
`/semantic/query` builds and discards a fresh engine per request, same
philosophy as `_query()`, eating the ~0.9s cost rather than risking a
design where the pipeline can never refresh while the API is running.

Building that fix surfaced two more bugs specific to MetricFlow's
programmatic API, both the "works once, breaks the process afterward"
kind — see ADR-12 for the full detail:

- `CLIConfiguration.setup()` resolves the DuckDB file path relative to
  the process CWD, not the project-dir argument passed to it (the same
  footgun `cdc/dbt/run.sh` exists to work around for the dbt CLI).
- Closing the engine's connection after a request needs more than
  `adapter.connections.cleanup_all()` — the object actually holding the
  raw connection (`DuckDBConnectionManager._ENV`) is a *class* attribute
  shared by the whole process, not per-instance state, so
  `close_all_connections()` has to be called too, or every other endpoint
  breaks for the rest of the process's life the first time
  `/semantic/query` is hit.

## Try it

```
# Point-and-shoot, via the API (assumes ./cdc/dbt/run.sh has populated cdc_raw.duckdb):
curl 'localhost:8000/semantic/query?metrics=order_count&group_by=order_id__status'
curl 'localhost:8000/semantic/query?metrics=total_revenue' \
  --data-urlencode "where={{ Dimension('order_id__channel') }} = 'web'" -G

# Or via the MetricFlow CLI directly, from cdc/dbt/:
cd cdc/dbt && ../../.venv/bin/mf query --metrics total_revenue,order_count \
  --group-by metric_time__day
```

Metrics and dimensions are declared in
`cdc/dbt/models/marts/_semantic.yml`, against `fct_cdc_orders`. Dimension
names follow MetricFlow's own convention of prefixing with the semantic
model's primary entity (`order_id__channel`, not just `channel`) — that's
MetricFlow's naming, not something this API invented.
