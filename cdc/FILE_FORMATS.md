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
from the records it's buffering. Confirmed directly against the jar (not
assumed): `format.output.type` accepts `parquet` as a real, working value on
the exact connector already deployed — switching is a one-line config
change (`connectors/s3-sink.json`), not a new integration.

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

## Where this leaves the format choice

|  | JSONL (today) | Parquet |
|---|---|---|
| Write-side simplicity | Best | Fine (connector already supports it) |
| Query pruning (column + row-group) | Path-only | Path + within-file |
| Storage size | Worst | Best |
| Self-describing schema | No | Yes, but per-file, with the union-schema wrinkle across files |
| Human-inspectable | Best | Needs tooling |
| Small-file behavior | Doesn't care | Known anti-pattern at scale (not yet a problem here) |

Nothing here is a slam-dunk either way at this playground's scale — the
performance/size advantages of Parquet are real but currently unmeasurable
on data this small, while JSONL's inspectability has had genuine, concrete
value throughout this project already. The place this stops being a wash is
specifically the dbt-on-raw-files work about to start: that's exactly the
scenario (a query engine reading the raw storage directly, repeatedly,
potentially at a larger scale later) where Parquet's read-side advantages
are the actual point, not a hypothetical one.

**Recommendation**: keep `file-sink` on JSONL (it's the quick, inspectable,
zero-dependency check; switching it to Parquet isn't even straightforward —
`FileStreamSinkConnector` has no format option, it would need a different
connector entirely). For `s3-sink`, switch `format.output.type` to `parquet`
before or alongside the dbt-on-raw-files work, since that's precisely where
the read-side difference stops being theoretical. Keep a way to regenerate a
JSONL sample on demand (`file-sink`, or a one-off `format.output.type=jsonl`
run) for whenever a human needs to eyeball something.
