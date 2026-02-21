import psycopg2
from src.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def init_db():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS merchant_activities (
                    event_id UUID PRIMARY KEY,
                    merchant_id     VARCHAR(20)    NOT NULL,
                    event_timestamp TIMESTAMP      NOT NULL,
                    product         VARCHAR(20)    NOT NULL,
                    event_type      VARCHAR(30)    NOT NULL,
                    amount          DECIMAL(18, 2) DEFAULT 0,
                    status          VARCHAR(10)    NOT NULL,
                    channel         VARCHAR(10),
                    region          VARCHAR(100),
                    merchant_tier   VARCHAR(10)
                );
            """)
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_status    ON merchant_activities (status);",
                "CREATE INDEX IF NOT EXISTS idx_product   ON merchant_activities (product);",
                "CREATE INDEX IF NOT EXISTS idx_merchant  ON merchant_activities (merchant_id);",
                "CREATE INDEX IF NOT EXISTS idx_ts_month  ON merchant_activities (DATE_TRUNC('month', event_timestamp));",
                "CREATE INDEX IF NOT EXISTS idx_prod_stat ON merchant_activities (product, status);",
            ]
            for sql in indexes:
                cur.execute(sql)
            conn.commit()
    finally:
        conn.close()

def is_data_loaded() -> bool:
    """Returns True if table has data — skip ingestion."""
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM merchant_activities LIMIT 1;")
            result = cur.fetchone()
        conn.close()
        return result is not None
    except Exception:
        return False