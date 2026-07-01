import sys
import logging

sys.path.insert(0, "/home/airflow/dags")
from stack_a_dwh_pipeline import (  # noqa: E402
    load_csv_to_bronze,
    transform_bronze_to_silver,
    aggregate_silver_to_gold,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_pipeline():
    logger.info("Starting integration test pipeline...")

    logger.info("Loading bronze tables...")
    load_csv_to_bronze("customers", "/home/airflow/data/raw/customers.csv")
    load_csv_to_bronze("products", "/home/airflow/data/raw/products.csv")
    load_csv_to_bronze("transactions", "/home/airflow/data/raw/transactions.csv")

    logger.info("Transforming to silver tables...")
    transform_bronze_to_silver("customers")
    transform_bronze_to_silver("products")
    transform_bronze_to_silver("transactions")

    logger.info("Aggregating to gold tables...")
    aggregate_silver_to_gold()

    logger.info("Pipeline OK")


if __name__ == "__main__":
    run_pipeline()
