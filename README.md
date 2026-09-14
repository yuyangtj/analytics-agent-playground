# Analytics Agent Playground

Two independent test arms, sharing only `generator/`'s entity/event logic:

- **Agent benchmark** (sections 1–5) — a synthetic e-commerce business in
  DuckDB with deliberately injected data-quality issues, and a benchmark
  measuring whether an analytics agent (1) answers business questions
  correctly and (2) recognizes when the data is missing, ambiguous, or
  unreliable rather than confidently answering anyway.
- **CDC pipeline** (section 6, `cdc/`) — the same business, but replayed as
  a stream of inserts/updates/deletes into Postgres, captured with
  Debezium/Kafka, and tested for whether a change-data-capture pipeline
  propagates those changes correctly and how much lag it introduces at
  each stage: streaming, batch, and raw-file/SQL.

Neither depends on the other's output.

```mermaid
flowchart TB
    subgraph benchmark["Agent benchmark (sections 1-5)"]
        GEN["generator/generate.py"] --> DUCK[("data/business.duckdb<br/>+ issue_log.json")]
        DUCK --> AGENT["agent/ (Claude/Kimi)"]
        DUCK --> DBT["dbt/: staging + marts"]
        AGENT --> GRADER["grader/ (correctness + awareness)"]
    end

    subgraph cdc["CDC pipeline (section 6, cdc/)"]
        EL["generator/eventlog.py"] -->|"cdc/replay.py, paced"| PG[("Postgres")]
        PG --> DBZ["Debezium / Kafka Connect"]
        DBZ --> SINKS["3 sinks: DuckDB, local file, MinIO/S3"]
        SINKS -->|"cdc/batch_load.py"| BATCH[("DuckDB, incremental")]
        SINKS -->|"cdc/dbt/, via httpfs"| CDCDBT["marts + metrics"]
        CDCDBT --> API["cdc/api/"] --> UI["cdc/frontend/"]
    end
```

See `cdc/README.md` for the CDC arm's own, more detailed diagram and
per-piece verification notes.

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
```

Provider credentials (only needed for the ones you use):

```bash
export ANTHROPIC_API_KEY=...          # for --provider claude

export KIMI_API_KEY=...               # for --provider kimi
export KIMI_BASE_URL=https://api.kimi.com/coding   # Kimi's Anthropic-compatible endpoint
```

---

## 1. Generate the database

```bash
.venv/bin/python -m generator.generate
```

Builds `data/business.duckdb` (customers, products, orders, order_items,
marketing_spend, sessions, returns, inventory) and injects 9 categories of
realistic data-quality issues on top of it — missing attribution, truncated
history, late-arriving data, duplicate records, category schema drift, currency
ambiguity, silent sampling, orphaned references, and incomplete inventory
snapshots. Ground truth for every injected issue is written to
`data/issue_log.json` (the grading answer key — never shown to the agent).

Regeneration is deterministic (fixed seed), so re-running produces the same data.

## 2. Ask the agent a question directly

```bash
.venv/bin/python -m agent.cli --provider kimi --db data/business.duckdb \
  --question "What was total revenue by acquisition channel?" --verbose
```

`agent/` is a minimal tool-use loop (`run_sql` over a read-only DuckDB connection)
that works against both Claude and Kimi K2/K3 via the same Anthropic Messages API
surface — provider selection is just `--provider claude|kimi` plus the matching env
vars. The system prompt is intentionally neutral (no hint to look for data
problems), and prompt caching is applied to the system prompt, tool schema, and the
growing conversation prefix.

Useful flags: `--model` (override the provider default), `--reasoning-effort
{low,high,max}` (Kimi k3/k3-256k only, default `low`), `--max-turns`,
`--system-prompt`.

## 3. Run the benchmark

```bash
.venv/bin/python -m benchmark.run --provider kimi --grade
```

Drives the agent through every question in `benchmark/questions.yaml` (20
questions: 5 clean, 11 issue-affected, 4 unanswerable), saves raw answers to
`benchmark/results/<run_id>.json`, and `--grade` chains straight into the grader
for an instant scorecard. Useful flags: `--limit N` and `--question-id <id>` for
quick smoke tests.

The agent only ever sees `data/business.duckdb` plus, if you choose to hand it
over, `benchmark/schema_readme.md` (honest column/type docs with zero disclosure
of which issues were injected).

## 4. Grade separately

```bash
.venv/bin/python -m grader.grade --answers benchmark/results/<run_id>.json
```

Scores each answer on two axes:
- **Correctness** — numeric answers extracted from the agent's text, matched
  against `expected_answer` within `tolerance`.
- **Awareness** — for questions with `requires_flag: true`, did the agent's answer
  contain language indicating it noticed the relevant limitation (keyword-based,
  see `grader/rubric.py`)? Also reports a false-positive rate: did it "cry wolf" on
  clean questions where nothing was wrong?

This is a deterministic/keyword grader, not an LLM judge — treat awareness scores
as a lower bound (an agent that flags an issue in unanticipated wording won't be
credited). `grader/fixtures/{good,naive}_answers.json` are reference answer sets
used to sanity-check the grader itself.

## 5. dbt models (optional modeling layer)

```bash
cd dbt && ../.venv/bin/dbt run --profiles-dir .
```

Builds a small staging + marts layer on top of `data/business.duckdb` (schemas
`main_staging` and `main_marts`): one staging model per raw table, plus
`dim_customers`, `dim_products`, and fact tables (`fct_orders`,
`fct_marketing_spend`, `fct_returns`, `fct_inventory_snapshots`).

**This is a faithful, non-cleaning pass-through** — no dedup, no NULL coalescing, no
dropping orphaned references, no status/date filtering. `fct_orders` specifically
uses `LEFT JOIN` (not `INNER JOIN`) to `orders`/`customers`/`products`, so an
orphaned `product_id`/`customer_id` shows up as a NULL join rather than being
silently dropped. The marts carry every injected issue through exactly as raw-table
queries would — they're not a "cleaned" alternative, just a differently-shaped view
of the same data.

**Important**: DuckDB uses file-level locking. `dbt run` opens
`data/business.duckdb` read-write, which conflicts with the agent's read-only
connection (or vice versa) if both try to access the file at the same time. Run
`dbt run` as a standalone step — not while `benchmark/run.py` or `agent/cli.py` is
active — and re-run it after `python -m generator.generate` if you want the marts
schemas refreshed (materialized as views, so they'll reflect new data automatically,
but a fresh DB file needs `dbt run` at least once to recreate the schemas in it).

---

## 6. CDC pipeline (optional)

```bash
cd cdc && docker compose up -d
.venv/bin/python -m generator.eventlog          # preview the replayable event stream
.venv/bin/python -m cdc.replay --speed 100000   # apply it to Postgres, paced
.venv/bin/python -m cdc.consumer --idle-exit 20 # materialize Kafka's view into DuckDB
.venv/bin/python -m cdc.verify                  # diff the sink against Postgres + report lag
```

`generator/eventlog.py` gives every row a lifecycle (inserts, corrections, late
arrivals, duplicates, retractions) instead of a single final state, and
`cdc/replay.py` applies that stream into Postgres, paced to real/scaled
time so `created_at`/`updated_at` reflect genuine arrival and correction
lag. Debezium/Kafka Connect streams the changes to three independent sinks
— a DuckDB materializer, a local JSONL file, and a MinIO/S3 bucket — and
two more independent paths sit on top of the bucket:

- **`cdc/batch_load.py`** — an incremental Python loader, tracking exactly
  which objects it's already processed (not a timestamp watermark, so it
  stays correct under late-arriving data) and writing to its own DuckDB
  file.
- **`cdc/dbt/`** — dbt-duckdb models reading the raw JSONL directly via
  `httpfs`, squashing the change-event log into current state and
  computing real metrics (revenue by channel, customers by region,
  inventory reorder status) in SQL, with no Python CDC-parsing code
  involved.

`cdc/api/` is a small FastAPI app serving those metrics over HTTP — it
never touches Kafka/Postgres/MinIO itself, only the DuckDB file `dbt run`
produces — including a `/meta` endpoint reporting when the marts were last
refreshed and how far behind the underlying data is right now. `cdc/frontend/`
is a small dashboard (plain HTML/JS, no build step) calling that API,
mounted same-origin so no CORS setup is needed.

See `cdc/README.md` for the full pipeline, what each piece verifies, and
known limitations; `cdc/ARCHITECTURE_DECISIONS.md` for the reasoning
behind the less-obvious choices. Fully independent of `data/business.duckdb`
and the sections above — nothing here touches the agent or the benchmark.

---

## Repo layout

```
generator/    builds the clean DB, then injects the 9 data-quality issues;
              generator/eventlog.py builds the CDC pipeline's replayable event stream
agent/        the tool-use analytics agent (run_sql loop, Claude/Kimi providers)
benchmark/    schema doc for the agent, questions.yaml, the run.py driver
grader/       scoring logic + CLI
dbt/          staging + marts modeling layer on top of data/business.duckdb
cdc/          Postgres + Debezium/Kafka Connect CDC pipeline
  replay.py, consumer.py, verify.py, batch_load.py, schema_map.py -- the
    Python side: replay, streaming consumer, correctness/lag checks,
    incremental batch loader, and the shared Debezium-envelope decode
    logic all three loaders import
  connect.Dockerfile, connectors/, docker-compose.yml -- Postgres,
    Debezium/Kafka Connect, MinIO, and the sink connector configs
  dbt/        models the raw MinIO storage directly via httpfs (a third,
              independent path onto the same data) -- staging squash
              models + marts/metrics; run.sh wraps `dbt run` to sidestep
              a dbt-duckdb path-resolution footgun
  api/        FastAPI app serving the marts over HTTP
  frontend/   plain HTML/JS dashboard calling the API
  README.md, ARCHITECTURE_DECISIONS.md, FILE_FORMATS.md -- pipeline docs,
              decision records, and the JSONL-vs-Parquet writeup
data/         generated DB + issue_log.json (gitignored, regenerate via generator.generate)
```

See `CHANGELOG.md` for what's implemented as of each tagged release
(`baseline-v1`, `cdc-v1`, `cdc-v2`, ...) and what's new incrementally at
each one.

## Regenerating questions.yaml

`benchmark/questions.yaml`'s expected answers are computed from a clean
(pre-injection) database build, not the served one:

```bash
.venv/bin/python -m benchmark.build_questions
```

Only needs re-running if you change `generator/config.py` (scale, date range) or
edit the question definitions in `benchmark/build_questions.py` directly.
