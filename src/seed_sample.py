
"""
Optional: Generate a small sample CSV for local testing when you don't
have the real dataset yet. Run with:

    python -m src.seed_sample

This will create data/activities_20240101.csv with 500 rows.
"""
import os
import uuid
import random
from datetime import datetime, timedelta

import pandas as pd

PRODUCTS = ["POS", "AIRTIME", "BILLS", "CARD_PAYMENT", "SAVINGS", "MONIEBOOK", "KYC"]
EVENT_TYPES = {
    "POS": ["CARD_TRANSACTION", "CASH_WITHDRAWAL", "TRANSFER"],
    "AIRTIME": ["AIRTIME_PURCHASE", "DATA_PURCHASE"],
    "BILLS": ["ELECTRICITY", "CABLE_TV", "INTERNET", "WATER", "BETTING"],
    "CARD_PAYMENT": ["SUPPLIER_PAYMENT", "INVOICE_PAYMENT"],
    "SAVINGS": ["DEPOSIT", "WITHDRAWAL", "INTEREST_CREDIT", "AUTO_SAVE"],
    "MONIEBOOK": ["SALE_RECORDED", "INVENTORY_UPDATE", "EXPENSE_LOGGED"],
    "KYC": ["DOCUMENT_SUBMITTED", "VERIFICATION_COMPLETED", "TIER_UPGRADE"],
}
STATUSES = ["SUCCESS", "FAILED", "PENDING"]
CHANNELS = ["POS", "APP", "USSD", "WEB", "OFFLINE"]
REGIONS = ["Lagos", "Abuja", "Kano", "Rivers", "Oyo", "Anambra"]
TIERS = ["STARTER", "VERIFIED", "PREMIUM"]

merchants = [f"MRC-{str(i).zfill(6)}" for i in range(1, 51)]

rows = []
base = datetime(2024, 1, 1)
for _ in range(500):
    product = random.choice(PRODUCTS)
    rows.append({
        "event_id": str(uuid.uuid4()),
        "merchant_id": random.choice(merchants),
        "event_timestamp": (base + timedelta(
            days=random.randint(0, 364),
            seconds=random.randint(0, 86399)
        )).isoformat(),
        "product": product,
        "event_type": random.choice(EVENT_TYPES[product]),
        "amount": round(random.uniform(0, 500000), 2),
        "status": random.choices(STATUSES, weights=[80, 15, 5])[0],
        "channel": random.choice(CHANNELS),
        "region": random.choice(REGIONS),
        "merchant_tier": random.choice(TIERS),
    })

os.makedirs("data", exist_ok=True)
df = pd.DataFrame(rows)
df.to_csv("data/activities_20240101.csv", index=False)
print(f"Sample CSV written: data/activities_20240101.csv ({len(rows)} rows)")