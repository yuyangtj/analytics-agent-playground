"""Builds a replayable event log (ordered INSERT/UPDATE/DELETE events) from the
same synthetic business as generator.generate, for CDC testing.

generator.generate writes one final snapshot straight into DuckDB -- there's
nothing for CDC to capture in that. This module treats each row's life as a
sequence of timestamped mutations (an INSERT when the row is created, then
zero or more UPDATE/DELETE events) so there's an actual change stream.
cdc/replay.py applies the resulting events to Postgres, paced to real/scaled
time, so created_at/updated_at end up reflecting genuine arrival/correction
lag rather than a replay-tool artifact (see cdc/README.md).

Mutation patterns layered on top of the base generated data:
  - late_insert:  a row's INSERT lands after its own domain date (an order
                  placed on day X doesn't hit the OLTP table until X+n) --
                  late-arriving data.
  - correction:   a row is inserted once, then UPDATEd later with revised
                  values (order status, return refund amount, marketing
                  spend restatement, product discontinuation).
  - duplicate:    the same logical row is INSERTed twice under different
                  surrogate keys -- upstream dedup not yet applied.
  - delete_after: a row is DELETEd some time after insert -- retractions.

Determinism: build_event_log() uses the same rng/faker seeding as
generator.generate, so the same config.SEED always produces the same ordered
event stream.
"""

import argparse
import json
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
from faker import Faker

from . import config, entities
from . import events as events_mod

# Mutation rates/lags -- local to the replayable path only; they don't affect
# generator.generate's DuckDB snapshot build.
LATE_INSERT_RATE = 0.03
LATE_INSERT_LAG_DAYS = (1, 10)

ORDER_STATUS_CORRECTION_LAG_DAYS = (1, 14)  # completed -> cancelled/refunded, as an UPDATE
RETURN_REFUND_CORRECTION_RATE = 0.05
RETURN_REFUND_CORRECTION_LAG_DAYS = (2, 10)
MARKETING_RESTATEMENT_RATE = 0.5  # most days get a "final numbers" pass
MARKETING_RESTATEMENT_LAG_DAYS = (2, 5)

DUPLICATE_RATE = 0.01  # applied separately to order_items and sessions
DELETE_RATE = 0.01  # applied separately to sessions and returns
DELETE_LAG_DAYS = (1, 30)

TABLE_PK = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "marketing_spend": "marketing_spend_id",
    "sessions": "session_id",
    "returns": "return_id",
    "inventory": "inventory_id",
}


@dataclass
class Event:
    event_ts: datetime
    table: str
    op: str  # "insert" | "update" | "delete"
    pk: dict[str, Any]
    payload: dict[str, Any] | None = None  # full row for insert, changed cols for update, None for delete
    seq: int = 0  # stable tiebreak for events sharing an event_ts

    def to_json(self) -> dict:
        return {
            "event_ts": self.event_ts.isoformat(),
            "table": self.table,
            "op": self.op,
            "pk": self.pk,
            "payload": self.payload,
            "seq": self.seq,
        }

    @staticmethod
    def from_json(d: dict) -> "Event":
        return Event(
            event_ts=datetime.fromisoformat(d["event_ts"]),
            table=d["table"],
            op=d["op"],
            pk=d["pk"],
            payload=d["payload"],
            seq=d["seq"],
        )


def _dt(d: date) -> datetime:
    """Domain dates are dates; event timestamps need time-of-day for ordering
    within a day, so spread them across business hours deterministically."""
    return datetime(d.year, d.month, d.day, 9, 0, 0)


def _native(v: Any) -> Any:
    """DataFrame round-trips leave numpy scalars (int64, float64, ...) in row
    dicts -- normalize to plain Python types so payloads are both
    JSON-serializable (write_jsonl) and directly usable as psycopg params
    (replay.py) without every caller needing to know about numpy."""
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.date()
    if hasattr(v, "item"):  # numpy scalar types (int64, float64, bool_, ...)
        return v.item()
    return v


class _EventBuilder:
    def __init__(self, rng: random.Random):
        self.rng = rng
        self.events: list[Event] = []
        self._seq = 0

    def add(self, event_ts: datetime, table: str, op: str, pk: dict, payload: dict | None = None):
        self._seq += 1
        clean_pk = {k: _native(v) for k, v in pk.items()}
        clean_payload = {k: _native(v) for k, v in payload.items()} if payload is not None else None
        self.events.append(Event(event_ts, table, op, clean_pk, clean_payload, self._seq))

    def maybe_late(self, base_ts: datetime) -> datetime:
        if self.rng.random() < LATE_INSERT_RATE:
            lag = self.rng.randint(*LATE_INSERT_LAG_DAYS)
            return base_ts + timedelta(days=lag)
        return base_ts

    def finish(self) -> list[Event]:
        self.events.sort(key=lambda e: (e.event_ts, e.seq))
        return self.events


def _emit_duplicate(b: _EventBuilder, event_ts: datetime, table: str, pk_field: str, dup_id: int, payload: dict):
    dup_payload = dict(payload)
    dup_payload[pk_field] = dup_id
    b.add(event_ts + timedelta(minutes=b.rng.randint(1, 120)), table, "insert", {pk_field: dup_id}, dup_payload)


def _after(ts: datetime, *parent_ts: datetime | None) -> datetime:
    """Clamp a child row's event_ts to strictly after every parent it
    references. Domain dates are drawn independently per table (a session's
    session_date has no relationship to its customer's signup_date), which
    Postgres's DuckDB-less cousin doesn't care about but a real FK constraint
    does: the parent's row must exist before the child references it. Without
    this, a customer/product delayed by maybe_late() (or just drawn with a
    later domain date than a row that references it) would make the child's
    INSERT fail with a foreign-key violation."""
    candidates = [ts] + [p + timedelta(seconds=1) for p in parent_ts if p is not None]
    return max(candidates)


def build_event_log(seed: int = config.SEED) -> list[Event]:
    rng = random.Random(seed)
    faker = Faker()
    faker.seed_instance(seed)
    b = _EventBuilder(rng)

    customers = entities.generate_customers(rng, faker)
    products = entities.generate_products(rng)
    marketing_spend = entities.generate_marketing_spend(rng)
    sessions = events_mod.generate_sessions(rng, customers)
    orders, order_items = events_mod.generate_orders_and_items(rng, customers, products)
    returns = events_mod.generate_returns(rng, orders, order_items)
    inventory = events_mod.generate_inventory(rng, products)

    # customers: insert only, at signup
    customer_insert_ts: dict[int, datetime] = {}
    for _, row in customers.iterrows():
        ts = b.maybe_late(_dt(row["signup_date"]))
        customer_insert_ts[row["customer_id"]] = ts
        b.add(ts, "customers", "insert", {"customer_id": row["customer_id"]}, row.to_dict())

    # products: inserted at launch without discontinued_date; discontinuation
    # (when present) applied as a later UPDATE, matching how it'd actually
    # happen operationally.
    product_insert_ts: dict[int, datetime] = {}
    next_order_item_id = 1
    for _, row in products.iterrows():
        payload = row.to_dict()
        discontinued_date = payload.pop("discontinued_date")
        ts = b.maybe_late(_dt(row["launch_date"]))
        product_insert_ts[row["product_id"]] = ts
        b.add(ts, "products", "insert", {"product_id": row["product_id"]}, {**payload, "discontinued_date": None})
        if discontinued_date is not None and hasattr(discontinued_date, "year"):
            b.add(
                _after(_dt(discontinued_date), ts),
                "products",
                "update",
                {"product_id": row["product_id"]},
                {"discontinued_date": discontinued_date},
            )

    # marketing_spend: preliminary numbers land same day; ~half get a "final
    # numbers" restatement a few days later (spend/impressions/clicks revised).
    next_marketing_id = 1
    marketing_id_by_row = {}
    for _, row in marketing_spend.iterrows():
        mid = next_marketing_id
        next_marketing_id += 1
        payload = {
            "marketing_spend_id": mid,
            "spend_date": row["date"],
            "channel": row["channel"],
            "spend": row["spend"],
            "impressions": row["impressions"],
            "clicks": row["clicks"],
        }
        ts = b.maybe_late(_dt(row["date"]))
        b.add(ts, "marketing_spend", "insert", {"marketing_spend_id": mid}, payload)
        if rng.random() < MARKETING_RESTATEMENT_RATE:
            lag = rng.randint(*MARKETING_RESTATEMENT_LAG_DAYS)
            revised = {
                "spend": round(float(row["spend"]) * rng.uniform(0.95, 1.08), 2),
                "impressions": int(row["impressions"] * rng.uniform(0.95, 1.08)),
                "clicks": int(row["clicks"] * rng.uniform(0.95, 1.08)),
            }
            b.add(_dt(row["date"]) + timedelta(days=lag), "marketing_spend", "update", {"marketing_spend_id": mid}, revised)

    # sessions: insert only (immutable clickstream), with a small rate of
    # both late arrival, duplication and after-the-fact deletion (e.g. bot
    # traffic purge).
    for _, row in sessions.iterrows():
        payload = row.to_dict()
        customer_id = row["customer_id"]  # may be NaN/None -- anonymous session, no FK to satisfy
        parent_ts = customer_insert_ts.get(customer_id) if pd.notna(customer_id) else None
        ts = _after(b.maybe_late(_dt(row["session_date"])), parent_ts)
        b.add(ts, "sessions", "insert", {"session_id": row["session_id"]}, payload)
        if rng.random() < DELETE_RATE:
            lag = rng.randint(*DELETE_LAG_DAYS)
            b.add(ts + timedelta(days=lag), "sessions", "delete", {"session_id": row["session_id"]})
        if rng.random() < DUPLICATE_RATE:
            dup_id = int(sessions["session_id"].max()) + row["session_id"]
            _emit_duplicate(b, ts, "sessions", "session_id", dup_id, payload)

    # orders: rows whose final generated status is completed are inserted
    # straight as completed. cancelled/refunded orders are inserted as
    # completed first, then UPDATEd to their final status -- a real order
    # lifecycle, not a status decided at row-creation time.
    order_status_by_id = dict(zip(orders["order_id"], orders["status"]))
    order_ts_by_id = {}
    for _, row in orders.iterrows():
        payload = row.to_dict()
        final_status = payload["status"]
        payload["status"] = "completed"
        ts = _after(b.maybe_late(_dt(row["order_date"])), customer_insert_ts.get(row["customer_id"]))
        order_ts_by_id[row["order_id"]] = ts
        b.add(ts, "orders", "insert", {"order_id": row["order_id"]}, payload)
        if final_status != "completed":
            lag = rng.randint(*ORDER_STATUS_CORRECTION_LAG_DAYS)
            b.add(ts + timedelta(days=lag), "orders", "update", {"order_id": row["order_id"]}, {"status": final_status})

    # order_items: inserted alongside their order's original insert (not the
    # possibly-later status-correction event).
    for _, row in order_items.iterrows():
        oiid = next_order_item_id
        next_order_item_id += 1
        payload = {"order_item_id": oiid, **row.to_dict()}
        # usually equal to the order's own insert ts; only later if this
        # line's product launched (was inserted) after the order was placed
        ts = _after(order_ts_by_id[row["order_id"]], product_insert_ts.get(row["product_id"]))
        b.add(ts, "order_items", "insert", {"order_item_id": oiid}, payload)
        if rng.random() < DUPLICATE_RATE:
            dup_id = next_order_item_id
            next_order_item_id += 1
            _emit_duplicate(b, ts, "order_items", "order_item_id", dup_id, payload)

    # returns: inserted at return_date; a slice get their refund_amount
    # revised after review.
    for _, row in returns.iterrows():
        payload = row.to_dict()
        ts = _after(b.maybe_late(_dt(row["return_date"])), order_ts_by_id.get(row["order_id"]))
        b.add(ts, "returns", "insert", {"return_id": row["return_id"]}, payload)
        if rng.random() < RETURN_REFUND_CORRECTION_RATE:
            lag = rng.randint(*RETURN_REFUND_CORRECTION_LAG_DAYS)
            revised = round(float(row["refund_amount"]) * rng.uniform(0.7, 1.0), 2)
            b.add(ts + timedelta(days=lag), "returns", "update", {"return_id": row["return_id"]}, {"refund_amount": revised})
        if rng.random() < DELETE_RATE:
            lag = rng.randint(*DELETE_LAG_DAYS)
            b.add(ts + timedelta(days=lag), "returns", "delete", {"return_id": row["return_id"]})

    # inventory: each weekly snapshot is its own row/insert. (A more
    # OLTP-realistic model would keep one mutable row per product+warehouse
    # and UPDATE it on every stock movement -- left as a future refinement;
    # for now each snapshot date is a distinct insert, matching the DuckDB
    # table's grain.)
    next_inventory_id = 1
    for _, row in inventory.iterrows():
        iid = next_inventory_id
        next_inventory_id += 1
        payload = {"inventory_id": iid, **row.to_dict()}
        ts = _after(b.maybe_late(_dt(row["snapshot_date"])), product_insert_ts.get(row["product_id"]))
        b.add(ts, "inventory", "insert", {"inventory_id": iid}, payload)

    return b.finish()


def write_jsonl(events: list[Event], path: str) -> None:
    with open(path, "w") as f:
        for e in events:
            f.write(json.dumps(e.to_json(), default=str) + "\n")


def read_jsonl(path: str) -> list[Event]:
    events = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(Event.from_json(json.loads(line)))
    return events


def main():
    parser = argparse.ArgumentParser(description="Build the replayable event log and (optionally) dump it to JSONL.")
    parser.add_argument("--out", default=None, help="Path to write the event log as JSONL. If omitted, just prints a summary.")
    parser.add_argument("--seed", type=int, default=config.SEED)
    args = parser.parse_args()

    events = build_event_log(seed=args.seed)
    by_table_op: dict[str, int] = {}
    for e in events:
        by_table_op[f"{e.table}:{e.op}"] = by_table_op.get(f"{e.table}:{e.op}", 0) + 1

    print(f"{len(events)} events, spanning {events[0].event_ts} to {events[-1].event_ts}")
    for k in sorted(by_table_op):
        print(f"  {k}: {by_table_op[k]}")

    if args.out:
        write_jsonl(events, args.out)
        print(f"Wrote {len(events)} events to {args.out}")


if __name__ == "__main__":
    main()
