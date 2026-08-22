"""Entrypoint: builds the clean synthetic database end to end.

Usage:
    python -m generator.generate
"""

import os
import random

import duckdb
from faker import Faker

from . import config, entities, events, inject_issues, schema


def build_database(db_path: str = config.DB_PATH, inject: bool = True) -> None:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    if os.path.exists(db_path):
        os.remove(db_path)

    rng = random.Random(config.SEED)
    faker = Faker()
    faker.seed_instance(config.SEED)

    print("Generating customers...")
    customers = entities.generate_customers(rng, faker)

    print("Generating products...")
    products = entities.generate_products(rng)

    print("Generating marketing_spend...")
    marketing_spend = entities.generate_marketing_spend(rng)

    print("Generating sessions...")
    sessions = events.generate_sessions(rng, customers)

    print("Generating orders and order_items...")
    orders, order_items = events.generate_orders_and_items(rng, customers, products)

    print("Generating returns...")
    returns = events.generate_returns(rng, orders, order_items)

    print("Generating inventory...")
    inventory = events.generate_inventory(rng, products)

    print(f"Writing to DuckDB at {db_path}...")
    con = duckdb.connect(db_path)
    schema.create_schema(con)

    for table_name, df in [
        ("customers", customers),
        ("products", products),
        ("orders", orders),
        ("order_items", order_items),
        ("marketing_spend", marketing_spend),
        ("sessions", sessions),
        ("returns", returns),
        ("inventory", inventory),
    ]:
        con.register("tmp_df", df)
        con.execute(f"INSERT INTO {table_name} SELECT * FROM tmp_df")
        con.unregister("tmp_df")
        print(f"  {table_name}: {len(df)} rows")

    if inject:
        print("\nInjecting data quality issues...")
        issue_log = inject_issues.apply_all(con)
        inject_issues.write_issue_log(issue_log)

    con.close()
    print("Done.")


if __name__ == "__main__":
    build_database()
