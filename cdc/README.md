# CDC pipeline

Postgres (OLTP source, logical replication on) → Debezium (Kafka Connect) → Kafka.
Reuses `generator/`'s entity/event logic to write and mutate rows over time so
there's something for CDC to actually capture — see `schema.sql` for the table
DDL and how it differs from the DuckDB copy in `generator/schema.py`.

## Bring it up

```bash
cd cdc
docker compose up -d
```

This starts, in order: Postgres (schema auto-applied from `schema.sql` on
first boot via `docker-entrypoint-initdb.d`), Zookeeper, Kafka, Kafka Connect
(Debezium's connect image, which bundles the Postgres connector plugin), and
`connector-init`, a one-shot container that registers `connector-postgres.json`
against Connect's REST API once it's healthy. Also starts `kafka-ui` at
http://localhost:8081 for browsing topics/messages while debugging.

Check the connector actually came up:

```bash
curl -s localhost:8083/connectors/postgres-source/status | jq
```

`.connector.state` and `.tasks[0].state` should both read `RUNNING`. If the
task is `FAILED`, `.tasks[0].trace` has the reason (common cause: Postgres
wasn't done applying `schema.sql` yet when Connect first tried to snapshot --
`connector-init` retries via the PUT being idempotent, but check the trace).

## What you get

One Kafka topic per table, named `business.public.<table>` (the
`topic.prefix` + schema + table, Debezium's default routing), each message a
Debezium change event with (since both converters run with
`schemas.enable=false`) the payload fields directly at the top level, not
wrapped in a `payload` key: `before`, `after`, `op` (`c`=create, `u`=update,
`d`=delete, `r`=read/snapshot) and `source` (LSN, transaction id, commit
timestamp `ts_ms` — what `cdc/consumer.py` uses for lag measurement).
Deletes also emit a tombstone (null-value) record afterward, per
`tombstones.on.delete`, matching standard Kafka log-compaction convention.
NUMERIC/DECIMAL columns need `decimal.handling.mode: double` (set in
`connector-postgres.json`) to come through as plain numbers rather than
base64-encoded bytes — that flag on the connector config controls value
*encoding*, separate from `schemas.enable`, which only controls whether the
schema is included alongside the value.

Connect's own state (offsets, connector config, task status) lives in three
internal topics (`cdc_connect_*`) — don't touch those, and don't write to a
`business.public.*` topic yourself, or Connect's compaction/offset tracking
gets confused about what it owns.

## Talking to it from outside compose

- Postgres: `localhost:5432`, db `business`, user `business`/`business` for
  direct writes (what `replay.py` will use), or `cdc_replication`/
  `cdc_replication` (replication-only, matches what the connector itself
  uses — don't write through this one).
- Kafka: `localhost:29092` (the `EXTERNAL` listener) for a consumer running
  on the host, e.g. the stream processor in a later stage. Containers talk to
  each other over `kafka:9092` instead.

## Resetting

```bash
docker compose down -v   # -v also drops pg-data, so the schema/publication/
                          # replication slot get recreated clean next `up`
```

A plain `docker compose down` (no `-v`) keeps Postgres data and the
replication slot around, which matters if you want to test what happens to
Debezium's snapshot/resume behavior across a restart — do that deliberately.

## Populating it: the event log + replay

`generator/eventlog.py` builds a replayable event stream on top of the same
entity/event logic `generator.generate` uses for the DuckDB snapshot, except
each row gets a lifecycle instead of a single final state: an INSERT when
it's created, then zero or more UPDATE/DELETE events (order status changes,
marketing spend restatements, refund corrections, late arrivals, duplicates,
retractions). See the module docstring for the full list of mutation
patterns. It's deterministic — same seed, same event stream — and FK-aware:
a child row's event never lands before the parent row it references.

```bash
# apply straight to Postgres (rebuilds the event log in-process)
.venv/bin/python -m cdc.replay --speed 100000

# or dump it first and replay from the file
.venv/bin/python -m generator.eventlog --out /tmp/events.jsonl
.venv/bin/python -m cdc.replay --events-file /tmp/events.jsonl --speed 100000

# fast, unpaced smoke test (no waiting, just checks the pipeline runs end to end)
.venv/bin/python -m cdc.replay --speed asap --limit 5000
```

`--speed` is simulated-seconds-per-real-second: `100000` (the default)
compresses the ~2-year generated date range into roughly 10 real minutes
while keeping every gap strictly ordered and still measurable (a 5-day
correction lag becomes ~4.3 real seconds, not instant) — this is what makes
`created_at`/`updated_at` genuinely reflect arrival/correction lag rather
than a replay-tool artifact, per the earlier design discussion. `asap`
disables pacing (events applied back to back): ordering is still correct,
but the *magnitude* of any lag collapses to near-zero, so use it for pure
mechanics/smoke tests, not lag measurement. `1` replays at true real-time
scale if you want to reproduce lag at the scale it'd actually happen at.

## Materializing + verifying: the consumer

`cdc/consumer.py` consumes the `business.public.*` topics and materializes
current table state into a local DuckDB file, applying each Debezium event
as an upsert (`c`/`u`/`r`) or delete (`d`, followed by the tombstone). It
also logs consumer lag (source commit `ts_ms` vs. when the message was
consumed) to a `_cdc_lag_log` table alongside the data.

```bash
# runs until 20s pass with no new messages -- fine for a batch/test run;
# omit --idle-exit to run indefinitely, like a real consumer service
.venv/bin/python -m cdc.consumer --idle-exit 20

# diff the materialized sink against live Postgres, and print lag percentiles
.venv/bin/python -m cdc.verify
```

`cdc/verify.py` checks two things per table: row parity (every PK in
Postgres should be in the sink, and vice versa -- a mismatch means either
the consumer hasn't caught up, or missed a delete) and the lag distribution
from `_cdc_lag_log`. Exits nonzero on any mismatch, so it's usable as a
smoke test.

**Requires `decimal.handling.mode: double`** on the connector (already set
in `connector-postgres.json`) -- without it, NUMERIC/DECIMAL columns arrive
as base64-encoded bytes rather than plain numbers.

**Consumer group note**: `cdc/consumer.py` defaults to `group.id
cdc-materializer` with offsets auto-committed. Re-running it with the same
group id after it's already caught up will see no new messages (that's
correct Kafka behavior, not a bug) -- pass a different `--group-id` for a
fresh read, or `--no-from-beginning` to only pick up what's new from here.

## Not done yet

- `inventory` snapshots are each their own INSERT (matching the DuckDB
  table's grain) rather than a single mutable row per product+warehouse
  updated on every stock movement — the latter would be more
  OLTP-realistic and is a reasonable future refinement.
- No sink besides the DuckDB materializer above (e.g. a Kafka Connect sink
  connector writing to files/S3, if you want to test that path specifically
  rather than a hand-rolled consumer).
