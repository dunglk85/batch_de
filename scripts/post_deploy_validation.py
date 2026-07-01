"""
Post-deployment validation: freshness, volume, null-rate checks.
Runs against a live database after deployment to verify pipeline health.
"""

import sys
import os
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

VALIDATION_EXIT = 1

# Baseline thresholds (tune per environment)
EXPECTED_MIN_ROWS = {
    "bronze_customers": 100,
    "bronze_products": 25,
    "bronze_transactions": 5000,
    "silver_customers": 100,
    "silver_products": 25,
    "silver_transactions": 4000,
    "gold_daily_sales_fact": 10,
    "gold_customer_metrics": 100,
    "gold_product_metrics": 10,
}

FRESHNESS_HOURS = 48
CRITICAL_NULL_COLUMNS = [
    ("silver_transactions", "transaction_id"),
    ("silver_transactions", "customer_id"),
    ("silver_customers", "customer_id"),
    ("silver_products", "product_id"),
]


def _ensure_psycopg2():
    try:
        import psycopg2  # noqa: F401
    except ImportError:
        import subprocess
        import sys

        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "psycopg2-binary==2.9.7", "--quiet"]
        )


def get_db_connection():
    try:
        _ensure_psycopg2()
        import psycopg2

        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "postgres"),
            database=os.getenv("DB_NAME", "airflow"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres123"),
        )
        return conn
    except Exception as e:
        logger.error(f"Cannot connect to database: {e}")
        return None


def _freshness_column(table: str) -> str:
    return "_ingestion_date" if table.startswith("bronze_") else "_updated_at"


def check_freshness(conn) -> bool:
    ok = True
    tables = ["bronze_transactions", "silver_transactions", "gold_daily_sales_fact"]
    cutoff = datetime.now() - timedelta(hours=FRESHNESS_HOURS)

    for table in tables:
        try:
            col = _freshness_column(table)
            cur = conn.cursor()
            cur.execute(f"SELECT MAX({col}) FROM stack_a.{table}")
            row = cur.fetchone()
            latest = row[0] if row and row[0] else None
            if latest is None:
                logger.warning(f"FRESHNESS: {table} has no timestamp data (new deployment)")
                cur.close()
                continue
            if latest < cutoff:
                logger.error(
                    f"FRESHNESS FAIL: {table} latest={latest} "
                    f"older than {FRESHNESS_HOURS}h cutoff"
                )
                ok = False
            else:
                logger.info(f"FRESHNESS OK: {table} latest={latest}")
            cur.close()
        except Exception as e:
            logger.error(f"FRESHNESS ERROR: {table}: {e}")
            conn.rollback()
            ok = False

    return ok


def check_volume(conn) -> bool:
    ok = True
    for table, min_rows in EXPECTED_MIN_ROWS.items():
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM stack_a.{table}")
            count = cur.fetchone()[0]
            if count < min_rows:
                logger.error(f"VOLUME FAIL: {table} has {count} rows (min {min_rows})")
                ok = False
            else:
                logger.info(f"VOLUME OK: {table} has {count} rows")
            cur.close()
        except Exception as e:
            logger.error(f"VOLUME ERROR: {table}: {e}")
            conn.rollback()
            ok = False
    return ok


def check_nulls(conn) -> bool:
    ok = True
    for table, column in CRITICAL_NULL_COLUMNS:
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM stack_a.{table} WHERE {column} IS NULL")
            null_count = cur.fetchone()[0]
            if null_count > 0:
                logger.error(f"NULL FAIL: {table}.{column} has {null_count} NULL rows")
                ok = False
            else:
                logger.info(f"NULL OK: {table}.{column} has 0 NULLs")
            cur.close()
        except Exception as e:
            logger.error(f"NULL ERROR: {table}.{column}: {e}")
            conn.rollback()
            ok = False
    return ok


def main():
    logger.info("=== Post-Deploy Validation ===")

    conn = get_db_connection()
    if conn is None:
        logger.error("Database unreachable — skipping post-deploy validation")
        sys.exit(VALIDATION_EXIT)

    try:
        freshness_ok = check_freshness(conn)
        volume_ok = check_volume(conn)
        nulls_ok = check_nulls(conn)

        logger.info(f"Freshness: {'PASS' if freshness_ok else 'FAIL'}")
        logger.info(f"Volume:    {'PASS' if volume_ok else 'FAIL'}")
        logger.info(f"Nulls:     {'PASS' if nulls_ok else 'FAIL'}")

        if freshness_ok and volume_ok and nulls_ok:
            logger.info("Post-deploy validation PASSED")
            sys.exit(0)
        else:
            logger.error("Post-deploy validation FAILED")
            sys.exit(VALIDATION_EXIT)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
