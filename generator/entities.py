"""Generation of base entities: customers, products, marketing_spend."""

import random
from datetime import timedelta

import numpy as np
import pandas as pd
from faker import Faker

from . import config


def _random_date(rng: random.Random, start, end):
    delta_days = (end - start).days
    return start + timedelta(days=rng.randint(0, delta_days))


def generate_customers(rng: random.Random, faker: Faker) -> pd.DataFrame:
    rows = []
    for cid in range(1, config.N_CUSTOMERS + 1):
        signup = _random_date(rng, config.START_DATE, config.END_DATE)
        rows.append(
            {
                "customer_id": cid,
                "signup_date": signup,
                "region": rng.choice(config.REGIONS),
                "acquisition_channel": rng.choice(config.ACQUISITION_CHANNELS),
                "email_domain": faker.free_email_domain(),
            }
        )
    return pd.DataFrame(rows)


def generate_products(rng: random.Random) -> pd.DataFrame:
    rows = []
    categories = list(config.CATEGORIES.items())
    for pid in range(1, config.N_PRODUCTS + 1):
        category, subcats = rng.choice(categories)
        subcategory = rng.choice(subcats)
        cost = round(rng.uniform(5, 300), 2)
        margin_multiplier = rng.uniform(1.3, 3.0)
        list_price = round(cost * margin_multiplier, 2)
        launch_date = _random_date(rng, config.START_DATE, config.END_DATE)

        discontinued_date = None
        # ~8% of products get discontinued at some point after launch
        if rng.random() < 0.08:
            max_disc = config.END_DATE
            if launch_date < max_disc:
                discontinued_date = _random_date(rng, launch_date, max_disc)

        rows.append(
            {
                "product_id": pid,
                "category": category,
                "subcategory": subcategory,
                "cost": cost,
                "list_price": list_price,
                "launch_date": launch_date,
                "discontinued_date": discontinued_date,
            }
        )
    return pd.DataFrame(rows)


def generate_marketing_spend(rng: random.Random) -> pd.DataFrame:
    rows = []
    d = config.START_DATE
    while d <= config.END_DATE:
        for channel in config.ACQUISITION_CHANNELS:
            if channel == "direct":
                # direct traffic has no associated spend
                continue
            base_spend = {
                "paid_search": 800,
                "social": 500,
                "email": 100,
                "organic_search": 0,
                "referral": 50,
            }.get(channel, 100)
            if base_spend == 0:
                continue
            spend = round(max(0, rng.gauss(base_spend, base_spend * 0.2)), 2)
            impressions = int(max(0, rng.gauss(spend * 50, spend * 10)))
            clicks = int(max(0, impressions * rng.uniform(0.01, 0.05)))
            rows.append(
                {
                    "date": d,
                    "channel": channel,
                    "spend": spend,
                    "impressions": impressions,
                    "clicks": clicks,
                }
            )
        d += timedelta(days=1)
    return pd.DataFrame(rows)
