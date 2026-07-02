from pyspark.sql import SparkSession
import logging
import os

logger = logging.getLogger(__name__)

DELTA_BASE = os.getenv("DELTA_LOCATION_BASE", "s3a://delta-lake")
BRONZE = f"{DELTA_BASE}/bronze"
SILVER = f"{DELTA_BASE}/silver"
GOLD = f"{DELTA_BASE}/gold"


def init_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("reconciliation")
        .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def count_table(spark: SparkSession, path: str) -> int:
    try:
        return spark.read.format("delta").load(path).count()
    except Exception:
        return -1


def main():
    spark = init_spark()
    tables = {
        "bronze/customers": f"{BRONZE}/customers",
        "bronze/products": f"{BRONZE}/products",
        "bronze/transactions": f"{BRONZE}/transactions",
        "silver/customers": f"{SILVER}/customers",
        "silver/products": f"{SILVER}/products",
        "silver/transactions": f"{SILVER}/transactions",
        "gold/daily_sales_fact": f"{GOLD}/daily_sales_fact",
        "gold/customer_metrics": f"{GOLD}/customer_metrics",
        "gold/product_metrics": f"{GOLD}/product_metrics",
    }
    counts = {}
    failures = 0
    for name, path in tables.items():
        cnt = count_table(spark, path)
        counts[name] = cnt
        if cnt < 0:
            logger.error(f"FAIL: {name} — table not found or empty")
            failures += 1
        else:
            logger.info(f"OK:   {name} — {cnt} rows")

    bronze_c = counts.get("bronze/customers", 0)
    silver_c = counts.get("silver/customers", 0)
    if bronze_c > 0 and silver_c > bronze_c:
        logger.error("FAIL: silver/customers has more rows than bronze/customers")
        failures += 1

    bronze_p = counts.get("bronze/products", 0)
    silver_p = counts.get("silver/products", 0)
    if bronze_p > 0 and silver_p > bronze_p:
        logger.error("FAIL: silver/products has more rows than bronze/products")
        failures += 1

    bronze_t = counts.get("bronze/transactions", 0)
    silver_t = counts.get("silver/transactions", 0)
    if bronze_t > 0 and silver_t > bronze_t:
        logger.error("FAIL: silver/transactions has more rows than bronze/transactions")
        failures += 1

    if failures:
        raise SystemExit(f"Reconciliation FAILED — {failures} failure(s)")
    logger.info("Reconciliation PASSED")
    spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    main()
