"""Generation parameters. Fixed seed for reproducibility."""

from datetime import date

SEED = 42

START_DATE = date(2024, 1, 1)
END_DATE = date(2025, 12, 31)  # 2-year window

N_CUSTOMERS = 2000
N_PRODUCTS = 500
N_ORDERS = 15000
N_SESSIONS = 40000

REGIONS = ["US-East", "US-West", "EU", "APAC", "LATAM"]
ACQUISITION_CHANNELS = ["organic_search", "paid_search", "social", "email", "referral", "direct"]
ORDER_STATUSES = ["completed", "cancelled", "refunded"]
ORDER_CHANNELS = ["web", "mobile_app", "marketplace"]
DEVICES = ["desktop", "mobile", "tablet"]
RETURN_REASONS = ["defective", "wrong_item", "no_longer_needed", "not_as_described", "other"]
WAREHOUSES = ["WH-East", "WH-West", "WH-EU"]

CATEGORIES = {
    "Electronics": ["Audio", "Wearables", "Accessories"],
    "Home": ["Kitchen", "Furniture", "Decor"],
    "Apparel": ["Menswear", "Womenswear", "Footwear"],
    "Beauty": ["Skincare", "Makeup", "Haircare"],
    "Outdoors": ["Camping", "Fitness", "Cycling"],
}

DB_PATH = "data/business.duckdb"
ISSUE_LOG_PATH = "data/issue_log.json"
