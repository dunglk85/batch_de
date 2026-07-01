import sys
import os
import logging

root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(root_dir, "dags"))
from stack_a_dwh_pipeline import (  # noqa: E402
    load_csv_to_bronze,
    transform_bronze_to_silver,
    aggregate_silver_to_gold,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_pipeline():
    logger.info("Starting integration test pipeline...")
    data_dir = os.path.join(root_dir, "data", "raw")

    logger.info("Loading bronze tables...")
    load_csv_to_bronze("customers", os.path.join(data_dir, "customers.csv"))
    load_csv_to_bronze("products", os.path.join(data_dir, "products.csv"))
    load_csv_to_bronze("transactions", os.path.join(data_dir, "transactions.csv"))

    logger.info("Transforming to silver tables...")
    transform_bronze_to_silver("customers")
    transform_bronze_to_silver("products")
    transform_bronze_to_silver("transactions")

    logger.info("Aggregating to gold tables...")
    aggregate_silver_to_gold()

    logger.info("Pipeline OK")


if __name__ == "__main__":
    run_pipeline()
