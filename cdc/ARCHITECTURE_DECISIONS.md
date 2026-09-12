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
