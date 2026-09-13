# Changelog

One entry per tagged release: roughly what's implemented at that point, and
(for everything after the first) what's new since the previous tag. Not a
commit-by-commit log — see `git log` for that.

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
