# Changelog

One entry per tagged release: roughly what's implemented at that point, and
(for everything after the first) what's new since the previous tag. Not a
commit-by-commit log — see `git log` for that.

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
