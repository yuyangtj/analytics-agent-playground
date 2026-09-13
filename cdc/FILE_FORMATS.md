# File formats for the raw-storage sinks

What `file-sink` and `s3-sink` actually write today, and whether Parquet is
worth switching to before building dbt models on top of the raw storage.
Written as reference material for that decision, not itself the decision —
see `ARCHITECTURE_DECISIONS.md` for the recorded outcome once there is one.

## What we use today: JSONL, not JSON

Worth being precise about this, since the two get conflated. **JSON** means
one document — typically one big object or array, which has to be
syntactically complete before anything can read it. **JSONL** ("JSON
Lines"/NDJSON) means one JSON value per line, each line independently valid.

Both sinks already write JSONL, not JSON, and that's not incidental —
`s3-sink`'s `format.output.type` is explicitly set to `jsonl` (verified
against the connector's own `FormatType` enum, which also lists `json`,
`csv`, `avro`, and `parquet` as the other valid values — `json` is a real,
different option, and would be the wrong one here). `file-sink`
(`FileStreamSinkConnector`) doesn't have a format setting at all — it just
appends `record.value().toString()` per record with a newline, which is
JSONL by construction, never plain JSON. This matters because JSONL is
*append-friendly* in a way JSON isn't: a streaming sink can write a new line
the instant a record arrives without knowing how many more are coming or
touching what's already on disk. A single JSON array can't be appended to
without rewriting the closing bracket (and everything after the last
element) every time — fundamentally the wrong shape for a sink that flushes
incrementally.

So the real question isn't "JSON or JSONL" — it's **JSONL or Parquet**.

## JSONL vs. Parquet

### Write side

JSONL: trivial. Each record serializes independently; the sink can flush at
any point with any number of buffered records. This is *why* it's the
natural choice for a Kafka Connect sink relative to something batched.

Parquet: not append-at-the-byte-level — it's a binary columnar format,
written in row-group batches with an embedded schema and per-column
statistics computed over the batch. The connector we're already running
(`s3-connector-for-apache-kafka`) can do this: it bundles `parquet-avro`,
`parquet-hadoop`, and friends, and writes Parquet via an Avro schema derived
from the records it's buffering.

**Correction, verified live rather than left as an assumption**: this
originally said switching was a one-line sink-side config change. It isn't.
Tested directly: pointing a `format.output.type=parquet` sink at our actual
schemaless topics fails immediately —
`SchemaProjectorException: Record must have schemas for key and value`.
Parquet is a typed columnar format; it cannot be written from a schemaless
`Map`, full stop. Confirmed the fix works by standing up an isolated second
source connector with `schemas.enable=true` on a throwaway topic prefix,
pointing a Parquet sink at *that*, and reading the result back with
`read_parquet()` — it produced a genuinely valid Parquet file with a proper
nested `STRUCT` schema (`before`/`after`/`source` as real typed columns, not
a blob). So Parquet output does work, and works well, but only once the
*source* connector carries real schemas — see "The real cost" below for
what that actually implies once accounted for correctly.

### Read side — this is where the earlier DuckDB discussion cashes out

This is the part that actually matters for the dbt-on-raw-files work coming
next. Recall from the partition-pruning discussion: DuckDB's hive-style
pruning on `dt=` works for *either* format, because it's based on the file
*path*, not contents. What's different is pruning *within* files:

- **JSONL**: no embedded statistics. To find out whether a file contains
  anything matching a filter, DuckDB has to parse it — there's no way to
  know without reading. No column pruning either: even a query touching one
  field has to parse the full line (Debezium's envelope, with nested
  `before`/`after`/`source`, isn't small) to extract it.
- **Parquet**: per-row-group min/max statistics let DuckDB skip whole
  row-groups without decoding them, on top of the same path-based
  partition pruning. Column pruning is native to the columnar layout — a
  query touching one field genuinely only reads that column's bytes, not
  the whole row.

For a small dataset like this playground's, the difference is invisible.
For "raw storage other consumers query later" (the actual stated purpose of
this sink, per ADR-1), it's the difference between a format built for that
and one that happens to work.

### Storage size

JSONL repeats every field *name* on every line — for Debezium's verbose
envelope (`before`, `after`, `source` with a dozen-odd sub-fields, `op`,
`ts_ms`, `ts_ns`, `ts_us`, `transaction`...), that's substantial overhead
per record, especially with `before` usually `null`. Parquet stores the
schema once per file and per-column encoding (dictionaries, run-length,
etc.) on top of that, plus a real compression codec on homogeneous columnar
data — which compresses meaningfully better than gzip-over-repetitive-JSON-
text would. Not measured on this dataset (small enough that it wouldn't be
a meaningful comparison), but this is the well-established general result,
not a guess specific to us.

### Schema handling — ties back to the schema-registry discussion

Parquet files are self-describing: each file embeds its own schema. That's
a partial, incidental answer to part of what we said a schema registry
would give us — a reader can at least tell *that* a file's schema differs
from another's, without needing `TABLE_COLUMNS` hardcoded anywhere. It's
still not compatibility *enforcement*, and it introduces a real wrinkle:
if the underlying table's shape changes between two `s3-sink` flushes,
you'd get two Parquet files with genuinely different embedded schemas for
the same logical topic. DuckDB can often paper over this at read time
(`union_by_name=true` on `read_parquet` merges differing schemas across
files), but it's a real seam to be aware of, not something Parquet makes
disappear.

### Human readability

JSONL wins outright, and this isn't a minor point for *this* project
specifically — we've spent real time in this conversation eyeballing raw
sample lines (`sample-events.jsonl`, `mc cat`-ing objects directly) to
verify things by hand. Parquet is binary; `mc cat`-ing a Parquet object
gives you nothing without `duckdb`/`parquet-tools`/pandas to open it first.
For a repo whose whole point is being inspectable, that's a real cost, not
just a rough edge.

### The small-file problem

`s3-sink` flushes on a ~60s offset-commit interval by default (or a
rebalance/shutdown). At this playground's data volumes, that produces small
objects. JSONL doesn't especially care — a small JSONL file is just a
shorter file. Parquet has real per-file fixed overhead (footer, schema,
statistics), so lots of small Parquet files is a known anti-pattern in real
data-lake setups (usually mitigated with a periodic compaction job merging
small files into larger ones) — something to be aware of if we do switch,
not necessarily something to solve now.

### The real cost: `schemas.enable=true` isn't scoped to `s3-sink`

This is the part the original version of this doc got wrong, and it changes
the shape of the decision. `schemas.enable` is a property of the *bytes on
the topic*, set by the connector that writes them — `postgres-source`, not
any individual sink. A sink can't unilaterally decide to parse an embedded
schema that was never written; the schema has to exist on the wire in the
first place. So switching `s3-sink` to Parquet means switching
`postgres-source` to `schemas.enable=true`, which affects *every* consumer
of `business.public.*`, not just this one sink:

- **`cdc/consumer.py` and `cdc/batch_load.py`** would break outright —
  both do `json.loads(raw_value)` and read `event["op"]`/`event["after"]`
  directly, assuming the flat shape. With schemas on, the JSON is wrapped in
  a `{"schema": ..., "payload": ...}` envelope (standard `JsonConverter`
  behavior, not a bug); every read site needs to unwrap `payload` first.
  This is exactly the code-change cost flagged in the earlier
  schema-registry discussion, now confirmed as the actual blocker for
  Parquet specifically, not just a hypothetical tradeoff.
- **`file-sink`** stays fine either way — it just echoes bytes through, so
  the schema envelope changes what a human sees in the file but breaks
  nothing.
- **Message size on every topic** increases — the full schema, inlined,
  on every message, not a compact registry-issued ID (there's still no
  registry here; see the earlier schema-registry conversation for why
  `schemas.enable=true` alone gets you the schema *present*, not
  compatibility *enforced*).

So this isn't "flip a flag on `s3-sink`." It's "commit to real schemas on
the whole pipeline," with `s3-sink`'s Parquet output as the payoff.

## Where this leaves the format choice

|  | JSONL (today) | Parquet |
|---|---|---|
| Write-side simplicity | Best | Fine, but only once the source carries real schemas |
| Blast radius to switch | None | `postgres-source` + every downstream reader (`consumer.py`, `batch_load.py`) |
| Query pruning (column + row-group) | Path-only | Path + within-file |
| Storage size | Worst | Best |
| Self-describing schema | No | Yes, but per-file, with the union-schema wrinkle across files |
| Human-inspectable | Best | Needs tooling |
| Small-file behavior | Doesn't care | Known anti-pattern at scale (not yet a problem here) |

Nothing here is a slam-dunk either way at this playground's scale — the
performance/size advantages of Parquet are real but currently unmeasurable
on data this small, while JSONL's inspectability has had genuine, concrete
value throughout this project already. What's changed since the first draft
of this doc is the actual price of admission: it's not a sink-side toggle,
it's a pipeline-wide commitment to schemas, with real code changes in two
places that currently assume schemaless JSON.

**Recommendation, revised**: don't switch the *main* pipeline's
`postgres-source` to `schemas.enable=true` just to get Parquet — that cost
(rewriting `consumer.py`/`batch_load.py`'s parsing, plus every message
getting bigger everywhere) is disproportionate to what this playground
actually needs to test right now. Two narrower options instead:
- **Keep `s3-sink` on JSONL**, accept that the dbt-on-raw-files work reads
  JSONL and only gets path-based partition pruning, not row-group/column
  pruning — still real, still worth building, just not the strongest
  version of the read-side story.
- **Stand up a second, schema-carrying source connector** on its own topic
  prefix (exactly what was used to verify this, live) purely for a Parquet
  arm, without touching the main pipeline's wire format or any existing
  reader's code. More infrastructure, but genuinely isolated — the pattern
  already proven to work.

Either way, keep `file-sink` on JSONL regardless (it's the quick,
inspectable, zero-dependency check; switching it to Parquet isn't even
possible — `FileStreamSinkConnector` has no format option at all).

## When to actually revisit this

The decision above is specific to this playground's current scale and
purpose, not a general claim that JSONL beats Parquet. Worth switching
`s3-sink` to Parquet (accepting the `schemas.enable=true` blast radius) if
any of these become true:

- **Data volume grows enough that storage/scan cost is real.** JSONL's
  per-record field-name repetition and lack of columnar compression are
  invisible at KBs-per-object; they stop being invisible at GB-TB scale, or
  anywhere query cost is billed per byte scanned (Athena, BigQuery).
- **Downstream queries typically touch a handful of fields out of many.**
  Parquet's column pruning only pays off when readers don't need the whole
  record every time. If every read wants the full envelope anyway, this
  advantage is moot.
- **The raw storage gets read many times, not once.** The read-side cost
  savings compound per read. A landing zone that's written once and rarely
  re-scanned gets little benefit from paying the write-side cost.
- **A real schema registry (Avro/Protobuf + Confluent-style registry) gets
  built for other reasons.** That's the scenario where the actual blocker
  here (no schema on the wire) disappears as a side effect of solving a
  different problem, and Parquet output becomes close to free.
- **The consuming ecosystem is Parquet-native.** Spark, Trino/Athena,
  Snowflake/BigQuery external tables, or any Iceberg/Delta/Hudi table
  format (all three are *built on* Parquet, not just compatible with it) —
  if that's who ends up reading this raw storage, Parquet stops being an
  optimization and becomes the price of entry.

**The one that actually applies to the very next thing being built**: none
of the above require touching `postgres-source` at all. The dbt-on-raw-files
work reads JSONL, dedups/squashes the CDC envelope into current state via
SQL, and *that* transformation's output — a plain query result, not
schemaless Debezium envelope JSON — can be materialized as Parquet by
`dbt-duckdb` trivially, since the schema problem that blocks the raw layer
doesn't exist at that layer at all. So the realistic shape isn't "JSONL vs.
Parquet at the landing zone" — it's the standard raw/bronze → curated/silver
split: **JSONL stays the landing format, Parquet becomes the transformation
step's *output* format**, sidestepping the blast-radius problem entirely
rather than waiting on any of the criteria above.
