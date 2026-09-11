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

## Not done yet

- `replay.py`: applies the generator's replayable event stream to Postgres
  through the `business` user. Nothing populates these tables yet.
- A consumer that materializes topic state into a queryable form (DuckDB
  sink or a small Python consumer) to diff against Postgres/the known event
  log for correctness and lag tests.
