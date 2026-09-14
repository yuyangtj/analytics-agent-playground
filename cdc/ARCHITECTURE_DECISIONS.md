# Architecture decisions

Lightweight ADRs for choices in `cdc/` that aren't obvious from the code alone —
mainly *why* things are shaped this way, for whoever touches this next
(including future us).

---

## ADR-1: Three sinks read the same Kafka topics independently, rather than one canonical path

**Status**: Implemented.

**Context**: Once Debezium is writing change events to Kafka, there's a design
choice about how a consumer gets from "message in a topic" to "usable data."
The simplest version is one path: a consumer reads the topic and writes
straight into a queryable database. That's exactly what `cdc/consumer.py` does,
materializing into DuckDB.

But this repo also has `file-sink` (plain local JSONL) and `s3-sink`
(partitioned JSONL in a MinIO/S3 bucket), reading the *same* topics
independently, with no dependency between the three.

**Decision**: Keep all three as parallel, independent sinks rather than
picking one "real" path.

**Why direct-to-database isn't the whole story**:

- **Coupling.** A consumer that goes straight to a database has to make every
  decision about the data *at ingestion time* — what the target schema looks
  like, how to upsert, how to handle a delete, what to do with a message
  it doesn't recognize yet. If that logic has a bug, or the schema needs to
  change, you're reprocessing history through code that's actively changing.
  `cdc/consumer.py` does take this path, and it works, but it's also the
  sink most exposed to that problem — see `verify.py`'s existence, which
  only exists because "does the DB sink actually match reality" isn't free
  to assume.
- **A staging file/object-storage step decouples ingestion from consumption.**
  Land the raw events first (file-sink, s3-sink), figure out the transformation
  logic later, and reprocess from the landed files as many times as needed
  without re-reading Kafka (whose retention is finite and whose replay speed
  is a shared resource) or replaying against Postgres again. This is the
  standard "raw/bronze layer" pattern in data lake architectures, and it's
  what real logical-replication-to-warehouse pipelines usually do rather than
  writing CDC events straight into a production table.
- **The raw files are also a shareable artifact, not just an internal detail.**
  Once events land in `s3-sink`'s bucket, any other consumer (a batch job, a
  different team, a notebook) can pick them up independently, on their own
  schedule, without touching Kafka, Postgres, or `cdc/consumer.py`'s Python
  process at all. `file-sink`'s plain JSONL is even lower-friction for that —
  no S3 client, no bucket credentials, just a file.

**Consequence**: three independent things can each be individually wrong.
`cdc/verify.py` only checks the DuckDB sink against Postgres; the file and S3
sinks are currently checked by eyeballing sample output (see the earlier
`sample-events.jsonl` review), not by an automated diff. That's a real gap —
worth an equivalent `verify`-style check for those two if they end up being
relied on rather than just demonstrated.

---

## ADR-2: The file/object-storage sinks are Hive-partitioned by event date

**Status**: Implemented (`cdc/connectors/s3-sink.json`).

**Context**: The first version of `s3-sink.json` wrote one object per topic,
total, forever: `{{topic}}/{{partition}}-{{start_offset}}.jsonl`. That's fine
for a quick correctness demo (which is what it was originally built for), but
it's the wrong shape for "raw event storage other people can consume later" —
a single ever-growing object per topic doesn't let a downstream reader scan
only the days they care about, doesn't compact well, and every consumer has
to read the whole history to find anything.

**Decision**: Partition object keys by event date, Hive-style
(`dt=YYYY-MM-DD/`), keyed off the Kafka record's own timestamp
(`timestamp.source: event`, not `wallclock` — the record's timestamp is set
by Debezium when it produced the message, close to when the underlying
Postgres change actually committed, which is the meaningful "when did this
happen" for partitioning, not whatever moment the sink connector happened to
process it):

```
business.public.<table>/dt=<yyyy>-<mm>-<dd>/<partition>-<start_offset>.jsonl
```

Verified live: a replay produced `business.public.customers/dt=2026-09-12/...`
etc. — one partition directory per day, correctly split by event date rather
than one flat pile.

**Consequence**: `file-sink` (plain `FileStreamSinkConnector`) is *not*
partitioned — it has no templating mechanism at all, it just appends to one
file forever. That's an accepted asymmetry: `file-sink` exists as the
lowest-friction "does data reach a file" check, `s3-sink` is the one meant to
resemble real raw-storage practice. If `file-sink` needs to become something
people actually build on, it should probably be retired in favor of a
partitioned S3/MinIO-shaped store rather than gaining its own partitioning
logic.

---

## ADR-3: Postgres, not DuckDB, is the CDC source

**Status**: Implemented (`cdc/schema.sql`, see also `cdc/README.md`).

**Context**: The rest of this repo already has a database — DuckDB
(`data/business.duckdb`). CDC could theoretically be tested against anything
with a change-notification mechanism.

**Decision**: Stand up a separate Postgres instance as the CDC source,
independent of DuckDB. Log-based CDC (Debezium/WAL-based tools) requires a
real transaction log with row-level before/after images; DuckDB is an
embedded analytical engine with no replication protocol, so it structurally
can't be a CDC source for a tool like Debezium. Postgres's logical
replication (`wal_level=logical`, publications, replication slots) is the
industry-standard mechanism these tools are built against.

**Consequence**: the two arms of this repo (agent benchmark vs. CDC pipeline)
are fully decoupled databases with separate schemas, sharing only
`generator/`'s entity/event *generation logic* — see the top-level README's
diagram. Data quality issues injected into the DuckDB copy for the benchmark
have no bearing on the CDC pipeline's Postgres data, and vice versa.

---

## ADR-4: Replayable event log instead of a one-shot snapshot

**Status**: Implemented (`generator/eventlog.py`, `cdc/replay.py`).

**Context**: `generator/generate.py` builds one final DuckDB snapshot in a
single bulk write. CDC has nothing to capture from a table that's written
once and never touched again.

**Decision**: `generator/eventlog.py` gives every row a *lifecycle* — an
INSERT when it's created, then zero or more UPDATE/DELETE events (order
status changes, marketing spend restatements, refund corrections, late
arrivals, duplicates, retractions) — and `cdc/replay.py` applies that stream
to Postgres paced to real/scaled time, so `created_at`/`updated_at` reflect
genuine arrival and correction lag rather than a replay-tool artifact.

**Consequence**: the event log is deterministic (same seed → same stream) and
FK-aware (a child row's event never lands before the parent it references —
see the note in `generator/eventlog.py`'s `_after()` helper about why that
clamping is necessary at all). This is also why `inventory` is modeled
differently in Postgres than in DuckDB: an OLTP inventory table has one
mutable row per product+warehouse, updated in place on every stock movement,
not a new row per weekly snapshot.

---

## ADR-5: A batch loader reading the same raw storage, incrementally, as a second path alongside the streaming consumer

**Status**: Implemented (`cdc/batch_load.py`, `cdc/schema_map.py`).

**Context**: `cdc/consumer.py` is a *streaming* consumer — a long-running
Kafka subscription applying each change as it arrives. ADR-1 already
justified landing raw events in `s3-sink`'s bucket independently of that
consumer, partly so *other* consumers could read them later, on their own
schedule, without touching Kafka at all. Until this ADR, nothing in this
repo actually was that other consumer — the raw storage's reusability was
asserted, not demonstrated.

**Decision**: `cdc/batch_load.py` reads directly from the `s3-sink` bucket
(not Kafka) and loads into its own DuckDB file, incrementally: it lists the
bucket's objects, diffs against a `_cdc_batch_load_state` table *inside that
same DuckDB file* (one row per already-loaded object key, with a
`processed_at` timestamp — a real history, not just a current set), and
only reads/applies whatever's new. Re-running it with nothing new is a
no-op; running it again after new objects land processes only those, not a
full rescan.

State lives in the same file as the data specifically so each object's
writes and its "processed" marker commit in one transaction
(`BEGIN`/`COMMIT` wrapping both) — a crash mid-object can't leave the data
applied but unmarked, or the reverse; it just leaves that one object
unprocessed, retried whole next run. An earlier version tracked state in a
separate local JSON file, updated with a plain file write *after* the
object's DB writes had already happened — safe (every apply is an upsert or
a keyed delete, both idempotent, so reprocessing was harmless) but not
exact, and it meant two artifacts instead of one. Moved into the same
DuckDB file once that gap was worth closing.

Object keys are the natural unit to track rather than a single date
watermark: `s3-sink` never rewrites an object once flushed (each flush
writes a *new* object, keyed by its starting Kafka offset), so multiple
objects can land under the same `dt=` partition over a day, and a
watermark of "processed through 2026-09-13" wouldn't tell you which of
that day's several objects you'd actually seen.

To avoid two independently-drifting copies of the table/column/type maps
(`cdc/consumer.py`'s original sin, before this ADR) both loaders now import
that logic from `cdc/schema_map.py` — one place that knows Debezium's
envelope shape, decode rules, and DuckDB schema, not two.

**Consequence**: this makes the batch-vs-streaming comparison ADR-1 gestured
at concrete rather than theoretical. Both loaders write to the same
`_cdc_lag_log` shape (tagged `loader='stream'` or `'batch'`), so the two are
directly comparable: streaming lag was consistently single-digit-to-tens of
seconds in testing; batch lag was ~70-80s, dominated by `s3-sink`'s ~60s
flush interval plus however long between batch runs. That gap *is* the
batch/streaming tradeoff, made visible with real numbers instead of asserted
in prose.

Verified live, in two passes (once for the JSON-file version, again after
moving state into `_cdc_batch_load_state`): replayed a slice, deleted one
row to exercise the tombstone path, ran the loader (all 8 tables matched
Postgres exactly, including the delete), ran it again with nothing new
(correctly a no-op), replayed more, and confirmed the next run processed
only the newly-landed objects — row counts matched Postgres exactly
throughout both passes, not just at the end. `cdc/verify.py --materialized
cdc/batch_materialized.duckdb` was run against the final state directly as
part of the second pass, confirming the transactional rework didn't change
the outcome, only how state is tracked.

---

## ADR-6: `s3-sink`'s output format (JSONL vs. Parquet)

**Status**: Investigated live; not switching for now. Full writeup in
`FILE_FORMATS.md`; this is the short version for the decision log.

**Context**: Both `file-sink` and `s3-sink` write JSONL today. That was
never a deliberate choice against Parquet — the connector was configured
for readability while everything else got built and verified by eyeballing
raw output. `s3-sink`'s connector (`s3-connector-for-apache-kafka`) bundles
full Parquet write support (`parquet-avro`, `parquet-hadoop`, ...), and
`parquet` is a real value for `format.output.type`.

**What testing this live actually found**: the first pass at this ADR
claimed switching was a one-line sink-side config change. Tested directly
and that was wrong — pointing a `parquet` sink at our real (schemaless)
topics fails immediately with
`SchemaProjectorException: Record must have schemas for key and value`.
Parquet can't be written from schemaless records. Confirmed the fix by
standing up an isolated second source connector with `schemas.enable=true`
on a throwaway topic and pointing a Parquet sink at that — it worked,
producing a genuinely valid, DuckDB-readable Parquet file with a proper
nested `STRUCT` schema. But `schemas.enable` is a property of the *source*
connector's output, shared by every consumer of that topic — switching it
on the real pipeline would break `cdc/consumer.py` and `cdc/batch_load.py`
(both parse the flat, schemaless JSON shape directly) and grow every
message on every topic, not just the ones headed to `s3-sink`.

**Decision**: not worth that blast radius just for one sink's file format.
Keep `s3-sink` on JSONL for the main pipeline; the dbt-on-raw-files work
gets path-based partition pruning (still real) but not row-group/column
pruning. If the Parquet read-side story is ever worth measuring for real,
the isolated second-source-connector pattern used to verify this is the
way to do it without touching the main pipeline's wire format.

---

## ADR-7: `cdc/dbt/` reads the raw storage directly, in SQL, with no incremental logic at all

**Status**: Implemented (`cdc/dbt/`).

**Context**: `cdc/consumer.py` and `cdc/batch_load.py` are two Python paths
onto the same underlying MinIO data. ADR-1 raised, and the file-formats
discussion confirmed, that a query engine reading the raw storage directly
is the scenario this repo hadn't actually built yet — only asserted was
possible.

**Decision**: `cdc/dbt/` is a third arm: plain dbt-duckdb models whose SQL
reads `s3-sink`'s JSONL directly off MinIO via DuckDB's `httpfs` extension.
Staging models (`stg_cdc_*`) squash the raw change-event log into current
state entirely in SQL (`row_number() over (partition by pk order by
source_ts_ms desc) = 1`, excluding rows whose latest `op` is `'d'`) — the
same logic `cdc/schema_map.py`'s `upsert`/`delete` implement procedurally in
Python, expressed declaratively instead. Marts on top
(`fct_cdc_orders`, `mart_revenue_by_channel`, `mart_customers_by_region`,
`mart_inventory_current`) are real metrics, not just a passthrough of the
staging layer.

**A real pitfall found and avoided, not just theorized**: DuckDB's
`read_json_auto` infers `before`/`after`'s type per read from whatever data
is actually present in that read — a table with zero deletes in the sample
gets `before` typed as generic `JSON`, a table with some deletes gets it
typed as a proper `STRUCT`. Struct dot-access (`after.customer_id`) and JSON
path access (`after->>'customer_id'`) are not interchangeable, so a model
built and tested against a data slice with no deletes could silently need
different SQL once deletes actually occur. Verified this directly before
writing any model (not assumed): explicit JSON-path extraction
(`value -> 'after' ->> 'col'`, forcing `columns={'value': 'JSON'}` at read
time rather than letting `read_json_auto` infer structs) works identically
regardless of what ops are present in a given read. Every staging model
uses this pattern uniformly.

**Deliberately no incremental logic**: every `dbt run` rescans everything
matching `dt=*/*.jsonl` in the bucket, in full — unlike `cdc/batch_load.py`'s
object-key state tracking. This is a real gap, accepted for now rather than
fixed, because dbt's natural incremental mechanism (a target-table
watermark) is exactly the pattern ADR-5 already rejected for
`batch_load.py`, for the same late-arrival reason: a timestamp-watermark
incremental model would silently miss an object that lands after the
watermark has already advanced past its internal timestamp. Replicating
`batch_load.py`'s exact-key tracking inside dbt/SQL is possible (anti-join
a glob's `filename=true` column against a dbt-managed table of already-seen
keys) but wasn't built here — bookkeeping `cdc/batch_load.py` already does,
in Python, correctly.

(**Note, superseding a line originally in this ADR**: this used to say
"querying a mart *is* the read," true when marts were `view`s. ADR-8
changed marts to `table`s so an API could serve them without triggering a
bucket rescan per request — the rescan now happens once per `dbt run`, not
once per query. Staging models are still views and still rescan per `dbt
run`; that part of this ADR is unchanged.)

**Verified live, not just "it ran without error"**: every staging table's
row count matched Postgres exactly (`customers`: 195, `sessions`: 1041 —
the `sessions` match specifically doubles as a delete-handling check, since
a missed delete would inflate it above Postgres's count); `orders` revenue
by status matched Postgres exactly (`completed`: 143 orders / $9925.46,
`cancelled`: 10 / $626.93, `refunded`: 11 / $99.67); `mart_revenue_by_channel`'s
total matched the same $9925.46/143 figure computed via an independently-
written Postgres aggregate (a plain `JOIN`+`SUM`, deliberately not the
dedup/squash query the dbt models use, so it isn't just checking the same
logic against itself).

**Consequence**: `cdc/dbt/profiles.yml` is a separate dbt project/profile
from `dbt/` (the benchmark arm) — different data source entirely (MinIO via
`httpfs`, vs. `data/business.duckdb` directly), matching the general
principle that the two arms of this repo stay decoupled, sharing only
`generator/`'s entity/event logic.

---

## ADR-8: marts materialize as tables, not views; a FastAPI app serves them

**Status**: Implemented (`cdc/api/`, `+materialized: table` on
`cdc_raw`'s marts in `dbt_project.yml`).

**Context**: ADR-7 built `cdc/dbt/`'s marts as `view`s — every query against
them re-reads the whole MinIO bucket. Fine for a human running `dbt run` and
poking at the result by hand. Not fine for a frontend dashboard hitting an
API repeatedly: every request would trigger a full bucket rescan through
several joined views, not a cheap read.

**Decision**: switch marts (not staging — that stays views, rescanning per
`dbt run` as before) to `+materialized: table`, and add `cdc/api/main.py`,
a small FastAPI app reading `cdc/dbt/cdc_raw.duckdb` read-only. This
splits the pipeline into a clear batch-refresh step (`dbt run`, rescans the
bucket, writes real tables) and a serving step (the API, reads whatever
tables already exist, fast, no S3 credentials or bucket access needed in
the API process at all).

**Endpoints**: `/health`, `/metrics/revenue-by-channel`,
`/metrics/customers-by-region`, `/metrics/inventory`, `/orders`
(paginated, filterable), `/orders/{order_id}` — each mapping close to one
mart, plus basic filter/pagination query params.

**Verified live**: every endpoint's output checked against an
independently-written Postgres aggregate, not just against the marts
themselves. `/metrics/revenue-by-channel?channel=mobile_app` summed to
`$3068.94`, matching a plain Postgres `JOIN`+`SUM` exactly; the row count
looked different at first (37 vs. 53) until accounting for the mart's
per-day grouping — `sum(order_count)` across those 37 rows reconciled to
the same 53 orders Postgres counted directly.

**A real DuckDB single-writer caveat, same one already documented in
`dbt/README.md` for the benchmark arm**: a `dbt run` (write) and an API
request (read) against the same file can't both hold it open. Each
endpoint opens its own short-lived read-only connection (rather than one
held for the app's lifetime) to minimize that window, and there's
defensive error-handling for it (`503` instead of a crash). **Not verified
live**: an actual concurrent-write test was attempted and didn't reproduce
the conflict — `dbt run`'s 12 tiny models finish in well under half a
second, apparently too fast to reliably overlap with a single test
request. Recorded honestly as unconfirmed, not silently assumed to work
because the code looks reasonable.

**A real footgun found and fixed after this ADR first shipped**:
`dbt-duckdb`'s `path: cdc_raw.duckdb` in `profiles.yml` resolves relative
to the *current working directory `dbt` is invoked from*, not
`--project-dir`. Running `dbt run --project-dir cdc/dbt --profiles-dir
cdc/dbt` from the repo root (a natural thing to try, since it's a common
enough dbt invocation style) silently created `cdc_raw.duckdb` at the repo
root instead — dbt reported success, no error, and the API then reported
`doesn't exist yet` for a `dbt run` that had just "succeeded," which is
exactly what happened. `cdc/dbt/run.sh` (new) fixes this properly rather
than just documenting around it: it `cd`s into `cdc/dbt` internally before
invoking `dbt`, so it can be called from anywhere (`./cdc/dbt/run.sh` from
the repo root) and the relative path always resolves the same way
regardless of the caller's own working directory. All docs and the API's
own error messages now point at `run.sh`, not a bare `dbt run`.

---

## ADR-9: the frontend is plain HTML/JS mounted same-origin on the API, not a separate app

**Status**: Implemented (`cdc/frontend/`, mounted via `StaticFiles` in
`cdc/api/main.py`).

**Context**: a frontend calling `cdc/api/`'s endpoints needs to be served
from *somewhere*. The two real options were a separate dev server (Vite,
plain `python -m http.server`, ...) with CORS configured on the API, or
serving the static files directly off the same FastAPI process the API
already runs.

**Decision**: same process, same origin. `cdc/api/main.py` mounts
`cdc/frontend/` at `/app` via Starlette's `StaticFiles`, registered *after*
every API route so it can't shadow one. This sidesteps CORS entirely —
`fetch('/metrics/...')` from a page served at `/app/` resolves against the
origin root regardless of the page's own path, no `Access-Control-*`
headers needed anywhere. Plain HTML/CSS/JS, no build step, no npm — matches
this repo's existing bias (nothing else here has a JS toolchain), and
Chart.js is loaded from a CDN rather than bundled.

**Verified**: every endpoint's actual JSON response checked field-by-field
against what `app.js` expects (types, the ISO-datetime format it truncates
with `.slice(0, 10)`, `needs_reorder`'s boolean-ness), and the exact
combined-filter query shapes the JS builds via `URLSearchParams`, re-run
directly against the live API outside the browser.

**Was not verified at merge time — recorded honestly rather than glossed
over — and that gap immediately caught a real bug**: no browser automation
tool was available in the environment this was built in
(`mcp__claude-in-chrome` reported no extension connected), so the page's
actual rendering was never observed directly before merging. The user
opened it in a real browser shortly after and sent a screenshot: both
charts were blank, and — more tellingly — the data table that should render
directly below the revenue chart was missing too, even though table
rendering doesn't depend on Chart.js at all. That combination pointed at
the chart call throwing and aborting the rest of the function before the
table-render call ever ran, not a data problem (the health line and filter
dropdowns in the screenshot were populated correctly).

Root cause, found by checking the CDN URL directly rather than guessing:
`Chart.js/4.4.4/chart.umd.min.js` 404'd — that exact version was never
published to cdnjs (confirmed via cdnjs's own API, which reports `4.5.1` as
current). Fixed the version pin, and separately hardened `app.js` itself:
added a `safeChart()` wrapper (`try`/`catch` around `new Chart(...)`) so a
chart failure — this exact CDN issue, an ad-blocker, any future hiccup —
can never again take the table in the same section down with it. Verified
the fix two ways: the corrected CDN URL returns `200` directly, and
`safeChart`'s catch behavior was unit-tested in isolation under Node with
`Chart` deliberately left undefined (simulating the exact failure that
happened), confirming it catches and returns `null` instead of throwing.
Visual confirmation of the actual fix still comes from the same source as
the bug report — the user, in a real browser — not from this environment.

---

## ADR-10: `/meta` reports two different kinds of staleness, computed without the API touching S3

**Status**: Implemented (`cdc/api/main.py`'s `/meta`,
`cdc/dbt/models/marts/mart_data_freshness.sql`).

**Context**: a dashboard showing metrics needs to say how current they are
— but "how current" is genuinely two different questions, and conflating
them would be misleading. "When did `dbt run` last refresh these tables"
tells you nothing about whether the pipeline upstream is healthy (you could
refresh stale-forever data all day and this number would look fine). "How
far behind is the underlying data" tells you nothing about whether anyone's
actually re-run `dbt` recently (the pipeline could be perfectly real-time
and the dashboard still show yesterday's numbers because nobody refreshed
the marts).

**Decision**: report both, separately. `marts_refreshed_at`/
`_ago_seconds` — `cdc_raw.duckdb`'s own file mtime, zero extra
infrastructure, computed in the API process directly (`os.path.getmtime`).
`data_as_of`/`data_lag_seconds` — the latest `updated_at` across all 8
source tables, via a new mart (`mart_data_freshness`) rather than a live
query: consistent with ADR-8's whole premise (the API never touches
MinIO/S3 itself), this aggregation runs against the staging views as part
of `dbt run`'s existing bucket-reading batch step, and the API just reads
the one-row-per-table result table like any other mart. `data_lag_seconds`
ends up folding the *entire* pipeline into one number — Postgres commit →
Debezium → Kafka → `s3-sink`'s flush interval → time since the last
`dbt run` — not because that's especially clever, but because that's
genuinely what "how stale is this row's business timestamp relative to
right now" already integrates over, for free.

**Verified live**: `data_as_of` matched `greatest(max(updated_at))` across
all 8 Postgres tables, computed independently, exactly to the microsecond;
`marts_refreshed_at` matched the DuckDB file's actual mtime on disk
(cross-checked against `stat`, accounting for the display timezone
difference — UTC in the API response vs. local time from `stat`, same
instant).

---

## ADR-11: `cdc/consumer.py`, `cdc/batch_load.py`, and `cdc/dbt/` are one deliberate ingestion-strategy comparison, not three unrelated components

**Status**: Implemented (`cdc/compare_ingestion.py`, `cdc/INGESTION_STRATEGIES.md`).

**Context**: these three pieces were each built for their own reason, at
different points, and documented separately (ADR-1, ADR-5, ADR-7). They
were never framed as answering one question together: streaming vs.
micro-batch vs. pure batch, same underlying data, what actually differs?
That framing turned out to be the more useful one — it's the actual
question a data platform decision between these approaches comes down to.

**Decision**: `cdc/compare_ingestion.py` reads whatever each of the three
has already produced (each one's own lag-tracking table, or the API's
`/meta` for the one that doesn't have a per-event lag concept at all) and
prints one consolidated report, rather than reimplementing lag tracking a
fourth time. `cdc/INGESTION_STRATEGIES.md` is the actual comparison —
architecture table, a real measured run, and the tradeoffs that don't show
up in the lag numbers (idle resource cost, what "lag" structurally means
per strategy, late-arrival correctness, restart behavior).

**A real methodology bug, hit and fixed while building this**: the first
comparison run used a Postgres instance replayed into across multiple
sessions hours apart. Kafka retained the old messages; the streaming
consumer read from-beginning and mixed hours-old backlog with fresh data
in one run, producing a p50 lag of *29 hours* — technically the output of
correct code, but not a fair comparison of anything. Not obvious at a
glance (a huge number doesn't announce itself as "contamination" rather
than "the strategy is just slow"); caught by checking the actual data's
timespan (`min`/`max(created_at)`) before trusting the numbers, then
re-running the entire comparison from a single clean teardown and one
contiguous replay session. Documented directly in
`cdc/INGESTION_STRATEGIES.md` rather than quietly fixed and forgotten,
since "don't compare lag across a stack with accumulated backlog" is a
real, reusable lesson for using this comparison tool at all, not specific
to this one run.

**Verified live**: the clean re-run showed a sane, expected gradient
(streaming p50 ~8s, micro-batch p50 ~54s — dominated by `s3-sink`'s own
~60s flush interval, pure-batch lag ~72s), and — checked independently,
not assumed — identical correctness across all three: `cdc/verify.py`
against both DuckDB outputs showed exact row-count parity with Postgres
on all 8 tables, and the dbt-sourced API's customer count matched Postgres
exactly (283 in all four places). So the comparison is genuinely only
about latency and operational shape, not a speed-vs-correctness tradeoff —
worth stating plainly rather than assuming "faster must mean less
correct" when the data doesn't show that here.
