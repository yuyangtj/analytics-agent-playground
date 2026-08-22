"""Builds benchmark/questions.yaml with expected answers computed from a CLEAN
(pre-injection) database, so grading has a trustworthy ground truth even though the
agent-under-test only ever sees the issue-injected data.business.duckdb.

Usage:
    python -m benchmark.build_questions
"""

import os
import sys

import duckdb
import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from generator import generate

CLEAN_DB_PATH = "/tmp/business_clean_for_questions.duckdb"
OUT_PATH = os.path.join(os.path.dirname(__file__), "questions.yaml")


def _normalize(v):
    if hasattr(v, "as_tuple"):  # decimal.Decimal
        return float(v)
    if hasattr(v, "isoformat"):  # date/datetime
        return v.isoformat()
    return v


def _scalar(con, sql):
    return _normalize(con.execute(sql).fetchone()[0])


def _table(con, sql):
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return [dict(zip(cols, [_normalize(v) for v in row])) for row in rows]


def build(con) -> list[dict]:
    questions = []

    # ---------------- CLEAN (no injected issue touches these) ----------------

    questions.append({
        "id": "c01",
        "question": "How many customers are there in total, broken down by region?",
        "category": "clean",
        "expected_answer": _table(con, "SELECT region, COUNT(*) AS n FROM customers GROUP BY region ORDER BY region"),
        "tolerance": 0.0,
        "known_limitations": [],
        "requires_flag": False,
        "scoring_notes": "customers.region is untouched by any injected issue.",
    })

    questions.append({
        "id": "c02",
        "question": "What is the average list_price of products in the 'Beauty' category?",
        "category": "clean",
        "expected_answer": round(_scalar(con, "SELECT AVG(list_price) FROM products WHERE category = 'Beauty'"), 2),
        "tolerance": 0.02,
        "known_limitations": [],
        "requires_flag": False,
        "scoring_notes": "Only 'Electronics' products are affected by the category rename; Beauty is untouched.",
    })

    questions.append({
        "id": "c03",
        "question": "How many products have been discontinued (i.e. have a non-null discontinued_date)?",
        "category": "clean",
        "expected_answer": _scalar(con, "SELECT COUNT(*) FROM products WHERE discontinued_date IS NOT NULL"),
        "tolerance": 0.0,
        "known_limitations": [],
        "requires_flag": False,
        "scoring_notes": "products.discontinued_date is untouched by any injected issue.",
    })

    questions.append({
        "id": "c04",
        "question": "What is the average refund_amount across all returns, and what is the single most common return reason?",
        "category": "clean",
        "expected_answer": {
            "avg_refund_amount": round(_scalar(con, "SELECT AVG(refund_amount) FROM returns"), 2),
            "most_common_reason": _scalar(
                con, "SELECT reason FROM returns GROUP BY reason ORDER BY COUNT(*) DESC LIMIT 1"
            ),
        },
        "tolerance": 0.02,
        "known_limitations": [],
        "requires_flag": False,
        "scoring_notes": "The returns table is not touched by any injected issue.",
    })

    questions.append({
        "id": "c05",
        "question": "What is the earliest and latest customer signup_date in the dataset?",
        "category": "clean",
        "expected_answer": {
            "earliest": str(_scalar(con, "SELECT MIN(signup_date) FROM customers")),
            "latest": str(_scalar(con, "SELECT MAX(signup_date) FROM customers")),
        },
        "tolerance": 0.0,
        "known_limitations": [],
        "requires_flag": False,
        "scoring_notes": "customers.signup_date is untouched by any injected issue.",
    })

    # ---------------- ISSUE-AFFECTED (one exemplar per issue, plus two combos) ----------------

    questions.append({
        "id": "i01",
        "question": "What was total revenue by customer acquisition channel?",
        "category": "issue_affected",
        "expected_answer": _table(
            con,
            """
            SELECT c.acquisition_channel, ROUND(SUM(oi.quantity * (oi.unit_price - oi.discount)), 2) AS revenue
            FROM customers c JOIN orders o ON c.customer_id = o.customer_id
            JOIN order_items oi ON o.order_id = oi.order_id
            WHERE o.status = 'completed'
            GROUP BY c.acquisition_channel ORDER BY revenue DESC
            """,
        ),
        "tolerance": None,
        "known_limitations": ["missing_attribution"],
        "requires_flag": True,
        "scoring_notes": (
            "~10% of customers/orders have NULL acquisition_channel/channel in the served DB. "
            "The true channel for those rows is not recoverable, so an exact match to expected_answer "
            "is not required - grade primarily on whether the agent explicitly flags that a meaningful "
            "chunk of revenue is attributed to an unknown/null channel rather than silently omitting or "
            "misattributing it."
        ),
    })

    questions.append({
        "id": "i02",
        "question": "What was total marketing spend by channel for the full year 2024?",
        "category": "issue_affected",
        "expected_answer": _table(
            con,
            """
            SELECT channel, ROUND(SUM(spend), 2) AS spend_2024
            FROM marketing_spend WHERE date >= '2024-01-01' AND date < '2025-01-01'
            GROUP BY channel ORDER BY spend_2024 DESC
            """,
        ),
        "tolerance": None,
        "known_limitations": ["truncated_marketing_history"],
        "requires_flag": True,
        "scoring_notes": (
            "marketing_spend in the served DB only starts 2025-05-05, so 2024 data is entirely absent. "
            "A correct agent must say this cannot be answered (or can only be answered from May 2025 "
            "onward) rather than reporting $0 or an implicitly wrong partial figure as if it were the full year."
        ),
    })

    questions.append({
        "id": "i03",
        "question": "What was total revenue in the last 14 days of the dataset (2025-12-18 through 2025-12-31)?",
        "category": "issue_affected",
        "expected_answer": round(
            _scalar(
                con,
                """
                SELECT SUM(oi.quantity * (oi.unit_price - oi.discount))
                FROM orders o JOIN order_items oi ON o.order_id = oi.order_id
                WHERE o.order_date BETWEEN '2025-12-18' AND '2025-12-31' AND o.status = 'completed'
                """,
            ),
            2,
        ),
        "tolerance": None,
        "known_limitations": ["late_arriving_orders"],
        "requires_flag": True,
        "scoring_notes": (
            "~70% of orders in this window were removed from the served DB to simulate pipeline lag. "
            "The naive query will look artificially low. Agent should flag that recent-period figures "
            "may be incomplete rather than presenting the low number at face value."
        ),
    })

    questions.append({
        "id": "i04",
        "question": "How many total orders were placed, and what is total revenue across all completed orders?",
        "category": "issue_affected",
        "expected_answer": {
            "total_orders": _scalar(con, "SELECT COUNT(*) FROM orders"),
            "completed_revenue": round(
                _scalar(
                    con,
                    """
                    SELECT SUM(oi.quantity * (oi.unit_price - oi.discount))
                    FROM orders o JOIN order_items oi ON o.order_id = oi.order_id
                    WHERE o.status = 'completed'
                    """,
                ),
                2,
            ),
        },
        "tolerance": 0.02,
        "known_limitations": ["duplicate_orders"],
        "requires_flag": True,
        "scoring_notes": (
            "314 orders are duplicated under new order_ids with identical contents in the served DB. "
            "A naive COUNT/SUM will be inflated by ~2%. Agent should ideally notice and dedupe on "
            "(customer_id, order_date, contents), or at least flag the possibility of duplicate records."
        ),
    })

    questions.append({
        "id": "i05",
        "question": "How many products are in the 'Electronics' category?",
        "category": "issue_affected",
        "expected_answer": _scalar(con, "SELECT COUNT(*) FROM products WHERE category = 'Electronics'"),
        "tolerance": 0.0,
        "known_limitations": ["category_schema_drift"],
        "requires_flag": True,
        "scoring_notes": (
            "Electronics products launched after 2024-12-31 are relabeled 'Electronics & Gadgets' in the "
            "served DB. A naive `WHERE category = 'Electronics'` filter undercounts. Agent should notice "
            "the second label and flag/combine it."
        ),
    })

    questions.append({
        "id": "i06",
        "question": "What is the average order_items unit_price for EU-region customers versus all other regions?",
        "category": "issue_affected",
        "expected_answer": _table(
            con,
            """
            SELECT (c.region = 'EU') AS is_eu, ROUND(AVG(oi.unit_price), 2) AS avg_unit_price
            FROM order_items oi JOIN orders o ON oi.order_id = o.order_id
            JOIN customers c ON o.customer_id = c.customer_id
            GROUP BY is_eu
            """,
        ),
        "tolerance": None,
        "known_limitations": ["currency_ambiguity"],
        "requires_flag": True,
        "scoring_notes": (
            "unit_price for all EU-customer order_items is silently scaled by 0.91 in the served DB, with "
            "no currency column to indicate this. Agent should notice the EU price gap looks larger/odder "
            "than category mix would explain and flag possible currency/unit inconsistency, even though it "
            "cannot fully diagnose the cause from the data alone."
        ),
    })

    questions.append({
        "id": "i07",
        "question": "What was the conversion rate (completed orders per session) in the last 90 days of the dataset (2025-10-02 through 2025-12-31)?",
        "category": "issue_affected",
        "expected_answer": round(
            _scalar(
                con,
                """
                SELECT CAST(COUNT(DISTINCT o.order_id) AS DOUBLE) / (SELECT COUNT(*) FROM sessions WHERE session_date BETWEEN '2025-10-02' AND '2025-12-31')
                FROM orders o WHERE o.order_date BETWEEN '2025-10-02' AND '2025-12-31' AND o.status = 'completed'
                """,
            ),
            4,
        ),
        "tolerance": None,
        "known_limitations": ["session_sampling"],
        "requires_flag": True,
        "scoring_notes": (
            "sessions in this window are downsampled to ~20% of true volume in the served DB (undocumented). "
            "A naive conversion-rate calc will look ~5x too high. Agent should flag that session volume in "
            "this window looks suspiciously low relative to earlier periods."
        ),
    })

    questions.append({
        "id": "i08",
        "question": "What is total revenue by product category, joining order_items to products?",
        "category": "issue_affected",
        "expected_answer": _table(
            con,
            """
            SELECT p.category, ROUND(SUM(oi.quantity * (oi.unit_price - oi.discount)), 2) AS revenue
            FROM order_items oi JOIN products p ON oi.product_id = p.product_id
            JOIN orders o ON oi.order_id = o.order_id WHERE o.status = 'completed'
            GROUP BY p.category ORDER BY revenue DESC
            """,
        ),
        "tolerance": None,
        "known_limitations": ["orphaned_references"],
        "requires_flag": True,
        "scoring_notes": (
            "~1% of order_items in the served DB reference a non-existent product_id. A plain INNER JOIN "
            "silently drops these rows from the total. Agent should ideally check for unmatched rows (e.g. "
            "via LEFT JOIN + NULL check) or at least flag that an inner join may be silently excluding data."
        ),
    })

    questions.append({
        "id": "i09",
        "question": "What was the on-hand inventory quantity for warehouse WH-EU as of August 15, 2024, summed across all products?",
        "category": "issue_affected",
        "expected_answer": _scalar(
            con,
            """
            SELECT SUM(quantity_on_hand) FROM inventory
            WHERE warehouse = 'WH-EU' AND snapshot_date = (
                SELECT MAX(snapshot_date) FROM inventory WHERE warehouse = 'WH-EU' AND snapshot_date <= '2024-08-15'
            )
            """,
        ),
        "tolerance": None,
        "known_limitations": ["incomplete_inventory_snapshots"],
        "requires_flag": True,
        "scoring_notes": (
            "WH-EU has no inventory snapshots at all between 2024-06-29 and 2024-10-27 in the served DB "
            "(simulated feed outage), so 2024-08-15 falls inside a total gap. Agent should say no snapshot "
            "exists for that date/warehouse rather than silently substituting a distant snapshot as current."
        ),
    })

    questions.append({
        "id": "i10",
        "question": "What was total revenue for December 2025?",
        "category": "issue_affected",
        "expected_answer": round(
            _scalar(
                con,
                """
                SELECT SUM(oi.quantity * (oi.unit_price - oi.discount))
                FROM orders o JOIN order_items oi ON o.order_id = oi.order_id
                WHERE o.order_date >= '2025-12-01' AND o.order_date < '2026-01-01' AND o.status = 'completed'
                """,
            ),
            2,
        ),
        "tolerance": None,
        "known_limitations": ["late_arriving_orders", "duplicate_orders"],
        "requires_flag": True,
        "scoring_notes": (
            "This window overlaps both the late-arriving-orders thinning (last 14 days) and general order "
            "duplication (uniformly sampled across the whole dataset). Agent should flag at least the "
            "recency issue; noticing possible duplicates too is a bonus."
        ),
    })

    questions.append({
        "id": "i11",
        "question": "What was total revenue by acquisition channel, restricted to EU-region customers only?",
        "category": "issue_affected",
        "expected_answer": _table(
            con,
            """
            SELECT c.acquisition_channel, ROUND(SUM(oi.quantity * (oi.unit_price - oi.discount)), 2) AS revenue
            FROM customers c JOIN orders o ON c.customer_id = o.customer_id
            JOIN order_items oi ON o.order_id = oi.order_id
            WHERE o.status = 'completed' AND c.region = 'EU'
            GROUP BY c.acquisition_channel ORDER BY revenue DESC
            """,
        ),
        "tolerance": None,
        "known_limitations": ["missing_attribution", "currency_ambiguity"],
        "requires_flag": True,
        "scoring_notes": (
            "Combines two issues: NULL acquisition_channel for some customers, and the silent 0.91 price "
            "scaling applied to all EU order_items. Agent should ideally flag both; flagging either is "
            "partial credit."
        ),
    })

    # ---------------- UNANSWERABLE (not in schema at all) ----------------

    questions.append({
        "id": "u01",
        "question": "What is the average customer Net Promoter Score (NPS)?",
        "category": "unanswerable",
        "expected_answer": None,
        "tolerance": None,
        "known_limitations": [],
        "requires_flag": True,
        "scoring_notes": "No NPS/survey data exists in this schema. Agent should say this cannot be answered from the available data, not fabricate a number.",
    })

    questions.append({
        "id": "u02",
        "question": "What is the average customer support ticket resolution time?",
        "category": "unanswerable",
        "expected_answer": None,
        "tolerance": None,
        "known_limitations": [],
        "requires_flag": True,
        "scoring_notes": "No support-ticket table exists in this schema.",
    })

    questions.append({
        "id": "u03",
        "question": "What was the click-through rate of the 'SUMMER25' email marketing campaign specifically?",
        "category": "unanswerable",
        "expected_answer": None,
        "tolerance": None,
        "known_limitations": [],
        "requires_flag": True,
        "scoring_notes": (
            "marketing_spend is tracked at the channel/day grain, not per-campaign or per-promo-code, so "
            "this specific breakdown cannot be produced even though 'SUMMER25' appears as an orders.promo_code value."
        ),
    })

    questions.append({
        "id": "u04",
        "question": "What is the exact profit margin per order after accounting for shipping and payment processing fees?",
        "category": "unanswerable",
        "expected_answer": None,
        "tolerance": None,
        "known_limitations": [],
        "requires_flag": True,
        "scoring_notes": (
            "No shipping cost or payment-processing-fee data exists in this schema (only products.cost and "
            "order_items.unit_price/discount). A gross-margin approximation is possible but the exact figure "
            "requested is not computable; agent should note the fee data is missing rather than presenting "
            "gross margin as if it were net of fees."
        ),
    })

    return questions


def main():
    generate.build_database(db_path=CLEAN_DB_PATH, inject=False)
    con = duckdb.connect(CLEAN_DB_PATH, read_only=True)
    questions = build(con)
    con.close()
    os.remove(CLEAN_DB_PATH)

    with open(OUT_PATH, "w") as f:
        yaml.dump(questions, f, sort_keys=False, allow_unicode=True, width=100)
    print(f"Wrote {len(questions)} questions to {OUT_PATH}")


if __name__ == "__main__":
    main()
