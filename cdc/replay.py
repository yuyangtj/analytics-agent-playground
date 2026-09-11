"""Applies the generator's replayable event log to Postgres, paced to real
(or scaled) time so created_at/updated_at genuinely reflect arrival and
correction lag -- see cdc/README.md and generator/eventlog.py.

Usage:
    # in-process: build the event log and replay it straight away
    python -m cdc.replay --speed 100000

    # replay a previously-dumped event log instead of rebuilding it
    python -m generator.eventlog --out /tmp/events.jsonl
    python -m cdc.replay --events-file /tmp/events.jsonl --speed 100000

    # smoke test: no pacing, no real writes
    python -m cdc.replay --limit 20 --speed asap --dry-run
"""

import argparse
import datetime
import os
import time

import psycopg

from generator import config, eventlog

DEFAULT_DB_URL = "postgresql://business:business@localhost:5432/business"

# Default speed compresses the ~2-year generated date range into roughly 10
# real minutes (65M simulated seconds / 100000 ~= 655s) while keeping every
# gap strictly ordered and still clearly nonzero -- a 5-day correction lag
# becomes ~4.3 real seconds, not instant. Use --speed asap for pure
# mechanics/smoke tests where lag magnitude doesn't matter, or a smaller
# number (down to 1.0 = real time) to reproduce lag at true scale.
DEFAULT_SPEED = 100_000.0


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _apply_event(cur: psycopg.Cursor, event: eventlog.Event) -> None:
    table = _quote_ident(event.table)

    if event.op == "insert":
        cols = list(event.payload.keys())
        col_sql = ", ".join(_quote_ident(c) for c in cols)
        placeholders = ", ".join(["%s"] * len(cols))
        values = [event.payload[c] for c in cols]
        cur.execute(f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})", values)

    elif event.op == "update":
        set_cols = list(event.payload.keys())
        set_sql = ", ".join(f"{_quote_ident(c)} = %s" for c in set_cols)
        where_sql = " AND ".join(f"{_quote_ident(c)} = %s" for c in event.pk)
        values = [event.payload[c] for c in set_cols] + list(event.pk.values())
        cur.execute(f"UPDATE {table} SET {set_sql} WHERE {where_sql}", values)

    elif event.op == "delete":
        where_sql = " AND ".join(f"{_quote_ident(c)} = %s" for c in event.pk)
        cur.execute(f"DELETE FROM {table} WHERE {where_sql}", list(event.pk.values()))

    else:
        raise ValueError(f"unknown op {event.op!r}")


def replay(
    events: list[eventlog.Event],
    db_url: str,
    speed: float | None,
    dry_run: bool = False,
    log_every: int = 2000,
) -> None:
    """speed=None means no pacing (apply events back to back)."""
    conn = None if dry_run else psycopg.connect(db_url, autocommit=True)
    try:
        cur = None if dry_run else conn.cursor()
        prev_ts: datetime.datetime | None = None
        started = time.monotonic()

        for i, event in enumerate(events, 1):
            if speed and prev_ts is not None:
                gap = (event.event_ts - prev_ts).total_seconds() / speed
                if gap > 0:
                    time.sleep(gap)
            prev_ts = event.event_ts

            if dry_run:
                if i <= 20 or i % log_every == 0:
                    print(f"[dry-run] {event.event_ts} {event.table}:{event.op} pk={event.pk}")
            else:
                try:
                    _apply_event(cur, event)
                except Exception as e:
                    print(f"ERROR applying event {i} ({event.table}:{event.op} pk={event.pk}): {e}")
                    raise

            if i % log_every == 0:
                elapsed = time.monotonic() - started
                print(f"  {i}/{len(events)} events applied ({elapsed:.1f}s elapsed, at {event.event_ts})")
    finally:
        if conn is not None:
            conn.close()

    print(f"Done: {len(events)} events replayed.")


def main():
    parser = argparse.ArgumentParser(description="Replay the event log into Postgres.")
    parser.add_argument("--db-url", default=os.environ.get("CDC_DB_URL", DEFAULT_DB_URL))
    parser.add_argument("--events-file", default=None, help="JSONL from `python -m generator.eventlog --out ...`. If omitted, builds the event log in-process.")
    parser.add_argument("--seed", type=int, default=config.SEED, help="Only used when building the event log in-process.")
    parser.add_argument(
        "--speed",
        default=str(DEFAULT_SPEED),
        help=(
            "Simulated-seconds-per-real-second multiplier (e.g. 1 = real "
            "time, 100000 = default). Pass 'asap' to disable pacing "
            "entirely and apply events back to back."
        ),
    )
    parser.add_argument("--limit", type=int, default=None, help="Only replay the first N events (smoke testing).")
    parser.add_argument("--start-at", default=None, help="Skip events before this ISO timestamp.")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be applied without touching Postgres.")
    parser.add_argument("--log-every", type=int, default=2000)
    args = parser.parse_args()

    speed = None if args.speed.strip().lower() == "asap" else float(args.speed)

    if args.events_file:
        events = eventlog.read_jsonl(args.events_file)
    else:
        events = eventlog.build_event_log(seed=args.seed)

    if args.start_at:
        start = datetime.datetime.fromisoformat(args.start_at)
        events = [e for e in events if e.event_ts >= start]
    if args.limit:
        events = events[: args.limit]

    print(f"Replaying {len(events)} events against {args.db_url if not args.dry_run else '(dry run)'}, speed={args.speed}")
    replay(events, db_url=args.db_url, speed=speed, dry_run=args.dry_run, log_every=args.log_every)


if __name__ == "__main__":
    main()
