"""Applies deliberate data-quality issues to the clean synthetic database.

Each `_issue_*` function mutates the DB via SQL and returns a dict describing what it
did (ground truth for grading). All entries are collected and written to
`data/issue_log.json` — this file is the answer key and must never be shown to the
agent under test.

Run standalone against an already-built DB:
    python -m generator.inject_issues
"""

import json
import random
from datetime import timedelta

import duckdb

from . import config


def _issue_null_attribution(con, rng: random.Random) -> dict:
    """Null out acquisition_channel/channel for a chunk of customers and orders."""
    con.execute(
        """
        UPDATE customers SET acquisition_channel = NULL
        WHERE customer_id IN (
            SELECT customer_id FROM customers USING SAMPLE 10% (bernoulli)
        )
        """
    )
    n_customers = con.execute(
        "SELECT COUNT(*) FROM customers WHERE acquisition_channel IS NULL"
    ).fetchone()[0]

    con.execute(
        """
        UPDATE orders SET channel = NULL
        WHERE order_id IN (
            SELECT order_id FROM orders USING SAMPLE 10% (bernoulli)
        )
        """
    )
    n_orders = con.execute("SELECT COUNT(*) FROM orders WHERE channel IS NULL").fetchone()[0]

    return {
        "id": "missing_attribution",
        "tables": ["customers", "orders"],
        "description": (
            f"{n_customers} customers have NULL acquisition_channel and {n_orders} orders have "
            "NULL channel (~10% each, randomly sampled). Any channel-attribution/ROI aggregation "
            "silently excludes or misclassifies these rows unless NULLs are handled explicitly."
        ),
    }


def _issue_truncated_marketing_history(con) -> dict:
    """marketing_spend only has data for the last 8 of the 24-month window."""
    cutoff = config.END_DATE - timedelta(days=8 * 30)
    before = con.execute("SELECT COUNT(*) FROM marketing_spend").fetchone()[0]
    con.execute("DELETE FROM marketing_spend WHERE date < ?", [cutoff])
    after = con.execute("SELECT COUNT(*) FROM marketing_spend").fetchone()[0]
    min_date = con.execute("SELECT MIN(date) FROM marketing_spend").fetchone()[0]

    return {
        "id": "truncated_marketing_history",
        "tables": ["marketing_spend"],
        "description": (
            f"marketing_spend rows before {cutoff.isoformat()} were removed ({before - after} of "
            f"{before} rows deleted). Earliest remaining record is {min_date}. Any spend trend or "
            "ROI question spanning the full date range cannot be answered before this cutoff — "
            "the absence of rows looks like zero spend, not missing data."
        ),
    }


def _issue_late_arriving_orders(con, rng: random.Random) -> dict:
    """Recent 14-day window has systematically incomplete orders (pipeline lag)."""
    window_start = config.END_DATE - timedelta(days=14)
    before = con.execute(
        "SELECT COUNT(*) FROM orders WHERE order_date >= ?", [window_start]
    ).fetchone()[0]

    con.execute(
        """
        DELETE FROM orders
        WHERE order_date >= ?
          AND order_id IN (
              SELECT order_id FROM orders WHERE order_date >= ? USING SAMPLE 70% (bernoulli)
          )
        """,
        [window_start, window_start],
    )
    after = con.execute(
        "SELECT COUNT(*) FROM orders WHERE order_date >= ?", [window_start]
    ).fetchone()[0]
    # keep order_items in sync
    con.execute(
        """
        DELETE FROM order_items
        WHERE order_id NOT IN (SELECT order_id FROM orders)
        """
    )

    return {
        "id": "late_arriving_orders",
        "tables": ["orders", "order_items"],
        "description": (
            f"Orders dated on/after {window_start.isoformat()} (last 14 days of the dataset) had "
            f"~70% of rows removed ({before - after} of {before}), simulating pipeline/reporting "
            "lag for recent data. Any 'recent period' aggregate will look artificially low unless "
            "the agent notices the falloff and flags recency as unreliable."
        ),
    }


def _issue_duplicate_orders(con, rng: random.Random) -> dict:
    """Duplicate ~2% of orders (new order_id, identical contents) plus their items."""
    max_order_id = con.execute("SELECT MAX(order_id) FROM orders").fetchone()[0]
    sample_orders = con.execute(
        "SELECT order_id FROM orders USING SAMPLE 2% (bernoulli)"
    ).fetchall()
    sample_ids = [r[0] for r in sample_orders]

    id_map = {}
    next_id = max_order_id + 1
    for oid in sample_ids:
        id_map[oid] = next_id
        next_id += 1

    for old_id, new_id in id_map.items():
        con.execute(
            """
            INSERT INTO orders SELECT ?, customer_id, order_date, status, channel, promo_code
            FROM orders WHERE order_id = ?
            """,
            [new_id, old_id],
        )
        con.execute(
            """
            INSERT INTO order_items SELECT ?, product_id, quantity, unit_price, discount
            FROM order_items WHERE order_id = ?
            """,
            [new_id, old_id],
        )

    return {
        "id": "duplicate_orders",
        "tables": ["orders", "order_items"],
        "description": (
            f"{len(id_map)} orders were duplicated under new order_ids with identical customer, "
            "date, and line items (simulating an upstream dedup failure). Naive COUNT/SUM revenue "
            "queries will be inflated by these duplicates unless deduplicated on "
            "(customer_id, order_date, contents)."
        ),
    }


def _issue_category_rename(con) -> dict:
    """Electronics category renamed to 'Electronics & Gadgets' for products launched after a cutoff."""
    cutoff = config.START_DATE + (config.END_DATE - config.START_DATE) // 2
    before = con.execute(
        "SELECT COUNT(*) FROM products WHERE category = 'Electronics'"
    ).fetchone()[0]
    con.execute(
        """
        UPDATE products SET category = 'Electronics & Gadgets'
        WHERE category = 'Electronics' AND launch_date > ?
        """,
        [cutoff],
    )
    after_renamed = con.execute(
        "SELECT COUNT(*) FROM products WHERE category = 'Electronics & Gadgets'"
    ).fetchone()[0]

    return {
        "id": "category_schema_drift",
        "tables": ["products"],
        "description": (
            f"Products in the 'Electronics' category launched after {cutoff.isoformat()} were "
            f"relabeled to 'Electronics & Gadgets' ({after_renamed} of {before} original Electronics "
            "products), simulating an in-flight taxonomy rename. A naive `WHERE category = "
            "'Electronics'` filter silently undercounts the category."
        ),
    }


def _issue_currency_ambiguity(con) -> dict:
    """EU-region customers' order_items unit_price is silently in a different currency (no column)."""
    before_avg = con.execute(
        """
        SELECT AVG(oi.unit_price) FROM order_items oi
        JOIN orders o ON oi.order_id = o.order_id
        JOIN customers c ON o.customer_id = c.customer_id
        WHERE c.region = 'EU'
        """
    ).fetchone()[0]

    con.execute(
        """
        UPDATE order_items SET unit_price = ROUND(unit_price * 0.91, 2)
        WHERE order_id IN (
            SELECT o.order_id FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            WHERE c.region = 'EU'
        )
        """
    )
    n_affected = con.execute(
        """
        SELECT COUNT(*) FROM order_items oi
        JOIN orders o ON oi.order_id = o.order_id
        JOIN customers c ON o.customer_id = c.customer_id
        WHERE c.region = 'EU'
        """
    ).fetchone()[0]

    return {
        "id": "currency_ambiguity",
        "tables": ["order_items"],
        "description": (
            f"unit_price for all {n_affected} order_items belonging to EU-region customers was "
            "silently scaled by 0.91 (simulating EUR figures stored without conversion or a "
            "currency column, previous blended avg unit_price was "
            f"{float(before_avg):.2f}). Revenue totals blending regions understate true value "
            "for EU orders; there is no column indicating this."
        ),
    }


def _issue_session_sampling(con) -> dict:
    """sessions table only contains a sample of true traffic for the most recent 3 months."""
    window_start = config.END_DATE - timedelta(days=90)
    before = con.execute(
        "SELECT COUNT(*) FROM sessions WHERE session_date >= ?", [window_start]
    ).fetchone()[0]

    con.execute(
        """
        DELETE FROM sessions
        WHERE session_date >= ?
          AND session_id IN (
              SELECT session_id FROM sessions WHERE session_date >= ? USING SAMPLE 80% (bernoulli)
          )
        """,
        [window_start, window_start],
    )
    after = con.execute(
        "SELECT COUNT(*) FROM sessions WHERE session_date >= ?", [window_start]
    ).fetchone()[0]

    return {
        "id": "session_sampling",
        "tables": ["sessions"],
        "description": (
            f"sessions dated on/after {window_start.isoformat()} were downsampled to ~20% of "
            f"original volume ({after} of {before} rows retained), simulating an undocumented "
            "sampling change in the tracking pipeline. Conversion-rate questions using this "
            "window will look artificially high unless the volume drop is noticed."
        ),
    }


def _issue_referential_integrity(con, rng: random.Random) -> dict:
    """Orphan a small % of order_items.product_id and orders.customer_id references."""
    max_product_id = con.execute("SELECT MAX(product_id) FROM products").fetchone()[0]
    fake_product_id = max_product_id + 9999

    sample_items = con.execute(
        "SELECT rowid FROM order_items USING SAMPLE 1% (bernoulli)"
    ).fetchall()
    con.execute(
        f"""
        UPDATE order_items SET product_id = ?
        WHERE rowid IN ({','.join(str(r[0]) for r in sample_items)})
        """,
        [fake_product_id],
    ) if sample_items else None
    n_orphan_items = con.execute(
        "SELECT COUNT(*) FROM order_items WHERE product_id = ?", [fake_product_id]
    ).fetchone()[0]

    max_customer_id = con.execute("SELECT MAX(customer_id) FROM customers").fetchone()[0]
    fake_customer_id = max_customer_id + 9999
    sample_orders = con.execute(
        "SELECT rowid FROM orders USING SAMPLE 1% (bernoulli)"
    ).fetchall()
    if sample_orders:
        con.execute(
            f"""
            UPDATE orders SET customer_id = ?
            WHERE rowid IN ({','.join(str(r[0]) for r in sample_orders)})
            """,
            [fake_customer_id],
        )
    n_orphan_orders = con.execute(
        "SELECT COUNT(*) FROM orders WHERE customer_id = ?", [fake_customer_id]
    ).fetchone()[0]

    return {
        "id": "orphaned_references",
        "tables": ["order_items", "orders"],
        "description": (
            f"{n_orphan_items} order_items reference a non-existent product_id ({fake_product_id}) "
            f"and {n_orphan_orders} orders reference a non-existent customer_id ({fake_customer_id}), "
            "simulating deleted-record/guest-checkout artifacts. INNER JOINs to products/customers "
            "silently drop these rows; only a LEFT JOIN with a NULL check surfaces them."
        ),
    }


def _issue_incomplete_inventory_snapshots(con) -> dict:
    """One warehouse is missing inventory snapshots entirely for a 4-month stretch."""
    gap_start = config.START_DATE + timedelta(days=180)
    gap_end = gap_start + timedelta(days=120)
    warehouse = "WH-EU"

    before = con.execute(
        "SELECT COUNT(*) FROM inventory WHERE warehouse = ? AND snapshot_date BETWEEN ? AND ?",
        [warehouse, gap_start, gap_end],
    ).fetchone()[0]
    con.execute(
        "DELETE FROM inventory WHERE warehouse = ? AND snapshot_date BETWEEN ? AND ?",
        [warehouse, gap_start, gap_end],
    )

    return {
        "id": "incomplete_inventory_snapshots",
        "tables": ["inventory"],
        "description": (
            f"All {before} inventory snapshot rows for warehouse '{warehouse}' between "
            f"{gap_start.isoformat()} and {gap_end.isoformat()} were removed, simulating a feed "
            "outage. Additionally, inventory is only snapshotted weekly (not real-time) for all "
            "warehouses. 'Current stock' or 'days of coverage' questions should note the snapshot "
            "may be stale (up to 7 days old) or, for WH-EU in this window, entirely missing."
        ),
    }


ISSUE_FUNCTIONS = [
    _issue_null_attribution,
    _issue_truncated_marketing_history,
    _issue_late_arriving_orders,
    _issue_duplicate_orders,
    _issue_category_rename,
    _issue_currency_ambiguity,
    _issue_session_sampling,
    _issue_referential_integrity,
    _issue_incomplete_inventory_snapshots,
]


def apply_all(con, seed: int = config.SEED) -> list[dict]:
    rng = random.Random(seed)
    issue_log = []
    for fn in ISSUE_FUNCTIONS:
        print(f"Applying issue: {fn.__name__}...")
        # functions that need randomness take rng; others don't
        if fn in (
            _issue_null_attribution,
            _issue_late_arriving_orders,
            _issue_duplicate_orders,
            _issue_referential_integrity,
        ):
            entry = fn(con, rng)
        else:
            entry = fn(con)
        issue_log.append(entry)
    return issue_log


def write_issue_log(issue_log: list[dict], path: str = config.ISSUE_LOG_PATH) -> None:
    with open(path, "w") as f:
        json.dump(issue_log, f, indent=2, default=str)
    print(f"Wrote issue log to {path} ({len(issue_log)} issues)")


if __name__ == "__main__":
    con = duckdb.connect(config.DB_PATH)
    log = apply_all(con)
    con.close()
    write_issue_log(log)
