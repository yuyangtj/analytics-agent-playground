"""Generation of event-level tables: sessions, orders, order_items, returns, inventory."""

import random
from datetime import timedelta

import pandas as pd

from . import config


def _random_date(rng: random.Random, start, end):
    delta_days = (end - start).days
    if delta_days <= 0:
        return start
    return start + timedelta(days=rng.randint(0, delta_days))


def generate_sessions(rng: random.Random, customers: pd.DataFrame) -> pd.DataFrame:
    customer_ids = customers["customer_id"].tolist()
    rows = []
    for sid in range(1, config.N_SESSIONS + 1):
        # ~15% of sessions are anonymous (no customer_id)
        customer_id = None if rng.random() < 0.15 else rng.choice(customer_ids)
        rows.append(
            {
                "session_id": sid,
                "customer_id": customer_id,
                "session_date": _random_date(rng, config.START_DATE, config.END_DATE),
                "channel": rng.choice(config.ACQUISITION_CHANNELS),
                "device": rng.choice(config.DEVICES),
            }
        )
    return pd.DataFrame(rows)


def generate_orders_and_items(rng: random.Random, customers: pd.DataFrame, products: pd.DataFrame):
    customer_ids = customers["customer_id"].tolist()
    product_ids = products["product_id"].tolist()
    product_prices = dict(zip(products["product_id"], products["list_price"]))

    order_rows = []
    item_rows = []

    for oid in range(1, config.N_ORDERS + 1):
        customer_id = rng.choice(customer_ids)
        order_date = _random_date(rng, config.START_DATE, config.END_DATE)
        status = rng.choices(config.ORDER_STATUSES, weights=[0.88, 0.07, 0.05])[0]
        channel = rng.choice(config.ORDER_CHANNELS)
        promo_code = rng.choice([None, None, None, "SAVE10", "WELCOME", "SUMMER25"])

        order_rows.append(
            {
                "order_id": oid,
                "customer_id": customer_id,
                "order_date": order_date,
                "status": status,
                "channel": channel,
                "promo_code": promo_code,
            }
        )

        n_items = rng.choices([1, 2, 3, 4], weights=[0.5, 0.3, 0.15, 0.05])[0]
        chosen_products = rng.sample(product_ids, k=min(n_items, len(product_ids)))
        for pid in chosen_products:
            quantity = rng.choices([1, 2, 3], weights=[0.7, 0.2, 0.1])[0]
            unit_price = float(product_prices[pid])
            discount = round(unit_price * rng.choice([0, 0, 0, 0.1, 0.2]), 2)
            item_rows.append(
                {
                    "order_id": oid,
                    "product_id": pid,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "discount": discount,
                }
            )

    return pd.DataFrame(order_rows), pd.DataFrame(item_rows)


def generate_returns(rng: random.Random, orders: pd.DataFrame, order_items: pd.DataFrame) -> pd.DataFrame:
    completed_orders = orders[orders["status"] == "completed"]
    order_dates = dict(zip(orders["order_id"], orders["order_date"]))
    item_totals = (
        order_items.assign(line_total=lambda df: df["quantity"] * (df["unit_price"] - df["discount"]))
        .groupby("order_id")["line_total"]
        .sum()
        .to_dict()
    )

    rows = []
    rid = 1
    for order_id in completed_orders["order_id"]:
        # ~6% of completed orders get a return
        if rng.random() < 0.06:
            order_date = order_dates[order_id]
            max_return_date = min(order_date + timedelta(days=30), config.END_DATE)
            return_date = _random_date(rng, order_date, max_return_date)
            order_total = float(item_totals.get(order_id, 0))
            refund_amount = round(order_total * rng.uniform(0.3, 1.0), 2)
            rows.append(
                {
                    "return_id": rid,
                    "order_id": order_id,
                    "return_date": return_date,
                    "reason": rng.choice(config.RETURN_REASONS),
                    "refund_amount": refund_amount,
                }
            )
            rid += 1
    return pd.DataFrame(rows)


def generate_inventory(rng: random.Random, products: pd.DataFrame) -> pd.DataFrame:
    """Weekly snapshots per product per warehouse."""
    rows = []
    snapshot_dates = []
    d = config.START_DATE
    while d <= config.END_DATE:
        snapshot_dates.append(d)
        d += timedelta(days=7)

    for _, product in products.iterrows():
        pid = product["product_id"]
        for warehouse in config.WAREHOUSES:
            on_hand = rng.randint(20, 500)
            reorder_point = rng.randint(10, 50)
            for snap_date in snapshot_dates:
                if snap_date < product["launch_date"]:
                    continue
                if pd.notna(product["discontinued_date"]) and snap_date > product["discontinued_date"]:
                    continue
                # random walk on stock level
                delta = rng.randint(-40, 30)
                on_hand = max(0, on_hand + delta)
                reserved = rng.randint(0, min(15, on_hand))
                rows.append(
                    {
                        "product_id": pid,
                        "warehouse": warehouse,
                        "snapshot_date": snap_date,
                        "quantity_on_hand": on_hand,
                        "quantity_reserved": reserved,
                        "reorder_point": reorder_point,
                    }
                )
    return pd.DataFrame(rows)
