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
http://localhost:8080 for browsing topics/messages while debugging.

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
Debezium change event: `payload.before`, `payload.after`, `payload.op`
(`c`=create, `u`=update, `d`=delete, `r`=read/snapshot) and `payload.source`
(LSN, transaction id, commit timestamp — useful for lag/ordering checks).
Deletes also emit a tombstone (null-value) record afterward, per
`tombstones.on.delete`, matching standard Kafka log-compaction convention.

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

## Not done yet

- A consumer that materializes topic state into a queryable form (DuckDB
  sink or a small Python consumer) to diff against Postgres/the known event
  log for correctness and lag tests.
- `inventory` snapshots are each their own INSERT (matching the DuckDB
  table's grain) rather than a single mutable row per product+warehouse
  updated on every stock movement — the latter would be more
  OLTP-realistic and is a reasonable future refinement.
