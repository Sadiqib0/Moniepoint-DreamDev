"""
CSV ingestion: reads all activities_YYYYMMDD.csv files from DATA_DIR → PostgreSQL.
"""
import os
import glob
import uuid
import logging

import pandas as pd
from psycopg2.extras import execute_values

from src.config import DATA_DIR
from src.database import get_connection

logger = logging.getLogger(__name__)

VALID_PRODUCTS = {"POS", "AIRTIME", "BILLS", "CARD_PAYMENT", "SAVINGS", "MONIEBOOK", "KYC"}
VALID_STATUSES = {"SUCCESS", "FAILED", "PENDING"}
VALID_CHANNELS = {"POS", "APP", "USSD", "WEB", "OFFLINE"}


def _clean_row(row):
    try:
        # Validate UUID
        event_id = str(uuid.UUID(str(row.get("event_id", "")).strip()))

        merchant_id = str(row.get("merchant_id", "")).strip()
        if not merchant_id:
            return None

        # Parse timestamp — skip rows where it's missing or unparseable (NaT)
        ts_raw = str(row.get("event_timestamp", "")).strip()
        event_timestamp = pd.to_datetime(ts_raw, errors="coerce")
        if pd.isnull(event_timestamp):          # catches NaT, NaN, empty, bad format
            return None
        if event_timestamp.tzinfo is not None:
            event_timestamp = event_timestamp.tz_localize(None)
        # Convert to plain Python datetime so psycopg2 handles it cleanly
        event_timestamp = event_timestamp.to_pydatetime()

        product = str(row.get("product", "")).strip().upper()
        if product not in VALID_PRODUCTS:
            return None

        event_type = str(row.get("event_type", "")).strip().upper()
        if not event_type:
            return None

        try:
            amount = float(str(row.get("amount", 0)).strip())
            if amount < 0:
                amount = 0.0
        except (ValueError, TypeError):
            amount = 0.0

        status = str(row.get("status", "")).strip().upper()
        if status not in VALID_STATUSES:
            return None

        channel = str(row.get("channel", "")).strip().upper() or None
        if channel and channel not in VALID_CHANNELS:
            channel = None

        region = str(row.get("region", "")).strip() or None
        merchant_tier = str(row.get("merchant_tier", "")).strip().upper() or None

        return {
            "event_id": event_id,
            "merchant_id": merchant_id,
            "event_timestamp": event_timestamp,
            "product": product,
            "event_type": event_type,
            "amount": round(amount, 2),
            "status": status,
            "channel": channel,
            "region": region,
            "merchant_tier": merchant_tier,
        }
    except Exception:
        return None


def _insert_batch(conn, rows):
    if not rows:
        return
    values = [
        (r["event_id"], r["merchant_id"], r["event_timestamp"],
         r["product"], r["event_type"], r["amount"],
         r["status"], r["channel"], r["region"], r["merchant_tier"])
        for r in rows
    ]
    sql = """
        INSERT INTO merchant_activities
            (event_id, merchant_id, event_timestamp, product, event_type,
             amount, status, channel, region, merchant_tier)
        VALUES %s
        ON CONFLICT (event_id) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=5000)
    conn.commit()


def load_all_csvs():
    pattern = os.path.join(DATA_DIR, "activities_*.csv")
    files = sorted(glob.glob(pattern))

    if not files:
        logger.warning("No CSV files found matching: %s", pattern)
        return 0, 0

    logger.info("Found %d CSV file(s) to ingest.", len(files))
    conn = get_connection()
    total_processed = 0
    total_inserted = 0

    try:
        for filepath in files:
            logger.info("Ingesting: %s", filepath)
            try:
                df = pd.read_csv(filepath, dtype=str, low_memory=False)
            except Exception as exc:
                logger.error("Could not read %s: %s", filepath, exc)
                continue

            batch = []
            for _, row in df.iterrows():
                total_processed += 1
                cleaned = _clean_row(row)
                if cleaned:
                    batch.append(cleaned)
                    if len(batch) >= 5000:
                        _insert_batch(conn, batch)
                        total_inserted += len(batch)
                        batch = []

            if batch:
                _insert_batch(conn, batch)
                total_inserted += len(batch)

            logger.info("  → %d rows processed from %s", len(df), os.path.basename(filepath))
    finally:
        conn.close()

    logger.info("Ingestion done. Processed: %d | Inserted: %d | Skipped: %d",
                total_processed, total_inserted, total_processed - total_inserted)
    return total_processed, total_inserted