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

**Deliberately no incremental logic**: every query against these models
rescans everything matching `dt=*/*.jsonl` in the bucket, every time —
unlike `cdc/batch_load.py`'s object-key state tracking. This is a real gap,
accepted for now rather than fixed, because dbt's natural incremental
mechanism (a target-table watermark) is exactly the pattern ADR-5 already
rejected for `batch_load.py`, for the same late-arrival reason: a
timestamp-watermark incremental model would silently miss an object that
lands after the watermark has already advanced past its internal
timestamp. Replicating `batch_load.py`'s exact-key tracking inside dbt/SQL
is possible (anti-join a glob's `filename=true` column against a
dbt-managed table of already-seen keys) but wasn't built here — bookkeeping
`cdc/batch_load.py` already does, in Python, correctly.

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
