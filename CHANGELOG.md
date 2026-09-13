# Changelog

One entry per tagged release: roughly what's implemented at that point, and
(for everything after the first) what's new since the previous tag. Not a
commit-by-commit log — see `git log` for that.

## `cdc-v4` — 2026-09-13

**Incremental since `cdc-v3`:**

- **`cdc/api/`** (new) — a FastAPI analytics API serving `cdc/dbt/`'s
  marts, meant to sit behind a frontend dashboard: `/health`,
  `/metrics/revenue-by-channel`, `/metrics/customers-by-region`,
  `/metrics/inventory`, `/orders` (paginated, filterable by
  `status`/`channel`), `/orders/{order_id}`. Reads `cdc/dbt/cdc_raw.duckdb`
  read-only; never touches Kafka, Postgres, or MinIO itself.
- **`cdc/dbt/`'s marts now materialize as `table`s, not `view`s** (staging
  stays views) — otherwise every API request would trigger a full MinIO
  bucket rescan through several joined views. `dbt run` is now the
  batch-refresh step; the API just reads whatever tables already exist.
  Recorded as ADR-8, which also updates ADR-7's now-stale "querying a mart
  is the read" line.
- Verified live: every endpoint checked against an independently-written
  Postgres aggregate, not just against the marts themselves. One thing
  attempted and honestly recorded as **not** verified: a live concurrent
  `dbt run`-vs-API-request race, meant to prove the `503` error-handling
  path for DuckDB's single-writer lock — `dbt run`'s models finish too
  fast to reliably overlap with a single test request, so the conflict
  wasn't actually reproduced. The defensive code stays in; it's marked
  unconfirmed rather than claimed proven.

## `cdc-v3` — 2026-09-13

**Incremental since `cdc-v2`:**

- **`cdc/dbt/`** (new) — a third, independent arm onto the raw CDC storage:
  plain dbt-duckdb models whose SQL reads `s3-sink`'s JSONL directly off
  MinIO via DuckDB's `httpfs` extension, no Python CDC-parsing code
  involved. Staging models (`stg_cdc_*`, one per table) squash the raw
  change-event log into current state entirely in SQL (dedup by PK ordered
  by `source_ts_ms`, excluding rows whose latest op is a delete). Marts on
  top (`fct_cdc_orders`, `mart_revenue_by_channel`,
  `mart_customers_by_region`, `mart_inventory_current`) compute real
  metrics, not just a passthrough. Verified live against Postgres, not
  just "it ran without error": every staging row count matched exactly,
  and revenue aggregates matched an independently-written Postgres query
  computed a different way. Deliberately has no incremental logic (every
  query rescans the whole bucket) — an accepted gap, not an oversight, for
  the same late-arrival reason `cdc/batch_load.py`'s object-key tracking
  exists in the first place. Recorded as ADR-7.
- **ADR-6 corrected** — the original claim that switching `s3-sink` to
  Parquet was a one-line config change was tested live and found wrong:
  Parquet requires real schemas on the wire (`schemas.enable=true` on
  `postgres-source`), which would break `cdc/consumer.py` and
  `cdc/batch_load.py` and grow every message on every topic, not just the
  ones headed to `s3-sink`. Confirmed the underlying premise still holds
  (Parquet's read-side benefits are real) by verifying it end-to-end
  against an isolated second source connector, then decided against
  paying that cost for the main pipeline.
- **`cdc/FILE_FORMATS.md`** gained a "when to actually revisit this"
  section — concrete trigger conditions (data volume, query patterns,
  repeated reads, an existing schema registry, a Parquet-native consuming
  ecosystem) for reopening the JSONL-vs-Parquet decision later, plus the
  reframe that resolves it for the immediate next step: Parquet's benefits
  are available at the dbt transformation's *output* layer without any of
  the blast radius, since a squash query's result isn't schemaless
  Debezium envelope JSON.

## `cdc-v2` — 2026-09-13

**Incremental since `cdc-v1`:**

- **Batch loading arm** (`cdc/batch_load.py`) — a second, independent path
  onto the same underlying data as `cdc/consumer.py`'s live Kafka consumer:
  reads `s3-sink`'s landed objects directly from MinIO/S3, incrementally,
  tracking exactly which object keys it's already loaded (not a timestamp
  watermark — deliberately, since a watermark would silently miss an
  object that lands after the watermark has already advanced past its
  internal timestamp, which is exactly what `generator/eventlog.py`'s
  late-arrival mutation produces). State lives in a table
  (`_cdc_batch_load_state`) inside its own output DuckDB file, with each
  object's data writes and its "processed" marker committing together in
  one transaction. Writes to the same `_cdc_lag_log` shape as the
  streaming consumer (tagged by a `loader` column), making the
  batch-vs-streaming lag comparison a real measured number rather than a
  claim in prose (streaming ~13-20s, batch ~70-80s in testing, dominated
  by the sink's flush interval).
- **`cdc/schema_map.py`** — the table/column/type maps and Debezium-envelope
  decode logic, factored out of `cdc/consumer.py` (which had it) and
  `cdc/verify.py` (which had an independent, drifting copy of `TABLE_PK`)
  into one shared module both loaders import.
- **`s3-sink` object keys are now date-partitioned** (Hive-style,
  `dt=YYYY-MM-DD/`, keyed off the Kafka record's own event timestamp) —
  the earlier version wrote one ever-growing object per topic forever,
  which isn't a usable shape for "raw storage other consumers can query
  later."
- **`cdc/ARCHITECTURE_DECISIONS.md`** (new) — lightweight ADRs recording
  the reasoning behind the less-obvious choices in `cdc/`: three
  independent sinks instead of one canonical database path, the object-key
  vs. timestamp-watermark tracking choice above, Postgres instead of
  DuckDB as the CDC source, the replayable event-log model, and (proposed,
  not yet implemented) switching `s3-sink` to Parquet.
- **`cdc/FILE_FORMATS.md`** (new) — JSONL vs. Parquet comparison for the
  raw-storage sinks, written ahead of the next arm (dbt models reading the
  raw files directly) since that's where the read-side difference (column
  pruning, row-group statistics on top of the path-based partition
  pruning both formats get) stops being theoretical.
- Architecture diagrams added to the top-level `README.md` and
  `cdc/README.md` (Mermaid, rendered natively on GitHub).

## `cdc-v1` — 2026-09-12

**Incremental since `baseline-v1`:**

- **dbt modeling layer** (`dbt/`) — staging + marts on top of
  `data/business.duckdb` (`main_staging`/`main_marts` schemas): one staging
  model per raw table, plus `dim_customers`, `dim_products`, and fact tables
  (`fct_orders`, `fct_marketing_spend`, `fct_returns`,
  `fct_inventory_snapshots`). A faithful, non-cleaning pass-through — every
  injected data-quality issue carries through unchanged, it's a
  differently-shaped view of the same data, not a cleaned alternative.

- **CDC test pipeline** (`cdc/`) — a second, independent test arm alongside
  the agent benchmark, testing change-data-capture correctness and lag
  rather than agent behavior:
  - `generator/eventlog.py` gives every row a lifecycle (inserts,
    corrections, late arrivals, duplicates, retractions) instead of a single
    final state; `cdc/replay.py` applies that stream to a Postgres OLTP
    source, paced to real/scaled time.
  - Postgres (`wal_level=logical`) → Debezium (Kafka Connect) → Kafka →
    three sinks reading the same topics independently: a DuckDB
    materializer (`cdc/consumer.py`, checked against Postgres by
    `cdc/verify.py`), a local JSONL file (`file-sink`), and a
    date-partitioned MinIO/S3 bucket (`s3-sink`).
  - `cdc/ARCHITECTURE_DECISIONS.md` records the reasoning behind the
    less-obvious choices (three independent sinks instead of one canonical
    database path, Postgres instead of DuckDB as the CDC source, the
    replayable event-log model, date partitioning on the object-storage
    sink).
  - Fully decoupled from the benchmark arm above: separate database
    (Postgres, not DuckDB), separate schema, shares only `generator/`'s
    entity/event generation logic.

Not carried over from the benchmark arm: no agent/grader changes, no new
questions in `benchmark/questions.yaml`.

## `baseline-v1` — 2026-08-22

Reference point for future comparisons (e.g. prompted vs. neutral agents,
different models, expanded question sets, transformation-layer variants).

- Synthetic e-commerce business generator (`generator/`): customers,
  products, orders, order_items, marketing_spend, sessions, returns,
  inventory, in DuckDB, deterministic (fixed seed).
- 9 categories of injected data-quality issues (missing attribution,
  truncated history, late-arriving data, duplicate records, category schema
  drift, currency ambiguity, silent sampling, orphaned references,
  incomplete inventory snapshots), with ground truth in `data/issue_log.json`
  (never shown to the agent).
- 20-question benchmark (`benchmark/questions.yaml`): 5 clean, 11
  issue-affected, 4 unanswerable, with expected answers computed from a
  clean pre-injection build.
- Keyword-based grader (`grader/`): correctness (numeric match within
  tolerance) and awareness (did the agent notice the relevant limitation),
  plus a false-positive rate on clean questions.
- Tool-use analytics agent (`agent/`): a `run_sql` loop against both Claude
  and Kimi K2/K3 via the same Anthropic Messages API surface, with prompt
  caching on the system prompt, tool schema, and growing conversation
  prefix.
