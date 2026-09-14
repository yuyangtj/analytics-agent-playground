# Ingestion strategies: streaming vs. micro-batch vs. pure batch

Three ways of getting the same CDC event stream into a queryable form,
already built and independently verified in this pipeline — reframed here
as one deliberate comparison rather than three components that happen to
exist. All three read off the exact same underlying data
(`generator/eventlog.py` → `cdc/replay.py` → Postgres → Debezium/Kafka →
`s3-sink`'s bucket, or the streaming/micro-batch pair reading Kafka
directly), so any difference between them is genuinely about the ingestion
*strategy*, not about different input.

## The three strategies

| | Streaming | Micro-batch | Pure batch |
|---|---|---|---|
| Code | `cdc/consumer.py` | `cdc/batch_load.py` | `cdc/dbt/` |
| Reads from | Kafka, continuously | `s3-sink`'s bucket, on demand | `s3-sink`'s bucket, on demand |
| Trigger | Long-running subscription | Invoked whenever (cron, manually, `--loop`) | Invoked whenever (`dbt run`) |
| State between runs | Kafka consumer group offsets | Object keys already processed (`_cdc_batch_load_state` table) | None at all |
| Rescans history on each run? | No — only new messages | No — only new objects | Yes — every matching object, every time |
| Lag measurement | Per-event (`_cdc_lag_log`, `loader='stream'`) | Per-event (`_cdc_lag_log`, `loader='batch'`) | Point-in-time only (`mart_data_freshness` / `/meta`) |

## What a clean, single-session run actually showed

Reproduced with `cdc/compare_ingestion.py` — one replay, then all three run
back to back with minimal gaps, so the comparison isn't contaminated by
Kafka retaining old messages across a long-lived dev session (an easy
mistake to make, and one this repo made once before writing this doc — see
"A real gotcha" below):

```
Streaming:    n=4995  min=5.1s   p50=8.0s   p95=9.3s   max=10.1s
Micro-batch:  n=4995  min=51.7s  p50=54.2s  p95=55.9s  max=56.1s
Pure batch:   data_as_of lag=71.9s (point-in-time, not a distribution)
```

A clean gradient, and an expected one: streaming has no polling interval
at all, so its lag is close to pure processing time. Micro-batch's lag is
dominated by `s3-sink`'s own ~60s flush interval — the batch loader itself
runs near-instantly once invoked, but it can only see what's already
landed in the bucket. Pure batch adds a second delay on top of that same
flush interval: however long since the last `dbt run` happened to occur.

**Correctness was identical across all three** — verified independently,
not assumed: `cdc/verify.py` against both `materialized.duckdb` and
`batch_materialized.duckdb` showed exact row-count parity with Postgres
across all 8 tables, and the dbt-sourced API's customer count matched
Postgres exactly too (283 = 283 = 283 = 283). So this comparison is purely
about **latency and operational shape** — none of the three strategies
sacrifices correctness for speed here. That's worth stating plainly rather
than assuming a "faster == less correct" tradeoff that this test doesn't
actually show.

## The tradeoffs that don't show up in the lag numbers

- **Operational cost while idle.** Streaming holds a live Kafka
  subscription open continuously — a running process, a consumer group,
  ongoing resource use whether or not anything's happening. Micro-batch
  and pure batch cost nothing between invocations; the tradeoff is paid
  in lag, not in idle resource use.
- **What "lag" even means differs structurally, not just numerically.**
  Streaming and micro-batch both log lag *per event* — a real distribution
  you can compute p50/p95/max over, because each one is applied and
  timestamped individually. Pure batch has no equivalent concept: `dbt
  run` either includes a row or it doesn't, and the only meaningful
  freshness number is "how stale is the newest row as of the last full
  run." `cdc/compare_ingestion.py` can't report a p95 for the pure-batch
  path because one doesn't exist to report — not a script limitation, a
  property of the strategy.
- **Late-arrival correctness.** ADR-5 already covers this in depth:
  micro-batch tracks exact object keys rather than a timestamp watermark,
  specifically because a watermark-based approach — the kind pure batch
  would need if it tried to add incremental logic — silently drops data
  that arrives after the watermark has already advanced past its internal
  timestamp. Streaming doesn't have this problem at all, since it
  processes Kafka's actual delivery order rather than reconstructing
  order from timestamps.
- **Failure/restart behavior.** Streaming resumes from Kafka's own
  committed offsets automatically. Micro-batch resumes from its own
  object-key state, also automatically. Pure batch has no notion of
  "resume" at all — a `dbt run` that's interrupted mid-way just means the
  next `dbt run` redoes the whole rescan; there's nothing to resume
  *from*, because there was never any partial progress being tracked in
  the first place.

## A real gotcha, hit while building this comparison

The first attempt at this comparison used a Postgres instance that had
been replayed into across multiple sessions hours apart. The streaming
consumer, reading from Kafka's beginning, mixed genuinely fresh messages
with hours-old backlog in one run — its lag stats were dominated by the
gap between an old replay and "now," not by anything about the streaming
strategy itself. Caught by checking the actual timespan of the data
(`select min(created_at), max(created_at)`) before trusting the numbers,
not by the numbers looking obviously wrong at a glance — a 29-hour p50
doesn't announce itself as contamination unless you go looking. Worth
remembering if you re-run this: a fair comparison needs a single,
contiguous replay session, not a stack that's accumulated backlog across
several unrelated runs.

## Reproducing this

```bash
cd cdc && docker compose up -d
cd .. && .venv/bin/python -m cdc.replay --speed asap --limit 5000

.venv/bin/python -m cdc.consumer --idle-exit 20
.venv/bin/python -m cdc.batch_load
./cdc/dbt/run.sh
.venv/bin/python -m cdc.api.main &   # needed for the pure-batch number

.venv/bin/python -m cdc.compare_ingestion
```

Run these back to back — a long gap between the replay and the three
loaders (or between the loaders themselves) reintroduces exactly the
contamination described above.
