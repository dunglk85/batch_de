"""
Stack B: Lakehouse Pipeline - Gold Layer
PySpark + Delta Lake: Analytics-ready aggregates and KPIs
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    current_timestamp,
    count,
    sum,
    avg,
    when,
    lit,
    min,
    max,
    round,
    datediff,
    row_number,
)
from pyspark.sql.window import Window
from datetime import datetime
import logging
import os

logger = logging.getLogger(__name__)

DELTA_BASE = os.getenv("DELTA_LOCATION_BASE", "s3a://delta-lake")
BRONZE = f"{DELTA_BASE}/bronze"
SILVER = f"{DELTA_BASE}/silver"
GOLD = f"{DELTA_BASE}/gold"


def init_spark_session(app_name: str = "stack_b_gold_aggregation") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config(
            "spark.delta.logStore.class",
            "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://dataops-minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "dataops-key")
        .config("spark.hadoop.fs.s3a.secret.key", "dataops-secret")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("INFO")
    return spark


class GoldLayerAggregator:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    # Daily Sales Fact

    def build_daily_sales_fact(self, load_date: str) -> int:
        logger.info("Building gold_daily_sales_fact")
        tx = (
            self.spark.read.format("delta")
            .load(f"{SILVER}/transactions")
            .filter(col("dq_is_valid"))
        )
        prd = (
            self.spark.read.format("delta")
            .load(f"{SILVER}/products")
            .select("product_id", "category")
        )

        df = (
            tx.join(prd, "product_id", "left")
            .withColumn("txn_date", to_date(col("transaction_date")))
            .groupBy("txn_date", "customer_id", "product_id", "category", "store_location")
            .agg(
                sum("quantity").alias("quantity"),
                sum(when(col("status") == "completed", col("amount")).otherwise(lit(0))).alias(
                    "gross_amount"
                ),
                sum(col("amount")).alias("net_amount"),
                count("*").alias("transaction_count"),
                count(when(col("status") == "completed", 1)).alias("completed_count"),
                count(when(col("status") == "refunded", 1)).alias("refunded_count"),
            )
            .withColumn("_updated_at", current_timestamp())
            .withColumn("_load_date", lit(load_date))
        )

        row_count = df.count()
        df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(
            f"{GOLD}/daily_sales_fact"
        )
        logger.info(f"Gold daily_sales_fact: {row_count} rows")
        return row_count

    # Customer Metrics

    def build_customer_metrics(self, load_date: str) -> int:
        logger.info("Building gold_customer_metrics")
        tx = (
            self.spark.read.format("delta")
            .load(f"{SILVER}/transactions")
            .filter(col("dq_is_valid"))
        )
        cst = (
            self.spark.read.format("delta")
            .load(f"{SILVER}/customers")
            .select("customer_id", "is_active")
        )
        prd = (
            self.spark.read.format("delta")
            .load(f"{SILVER}/products")
            .select("product_id", "category")
        )

        tx_with_cat = tx.join(prd, "product_id", "left")

        customer_tx = tx.groupBy("customer_id").agg(
            sum("amount").alias("customer_lifetime_value"),
            count("*").alias("total_transactions"),
            min(to_date("transaction_date")).alias("first_purchase_date"),
            max(to_date("transaction_date")).alias("last_purchase_date"),
            avg("amount").alias("avg_transaction_amount"),
        )

        preferred_category = (
            tx_with_cat.groupBy("customer_id", "category")
            .agg(count("*").alias("cat_count"))
            .withColumn(
                "rn",
                row_number().over(
                    Window.partitionBy("customer_id").orderBy(col("cat_count").desc())
                ),
            )
            .filter(col("rn") == 1)
            .select("customer_id", col("category").alias("preferred_category"))
        )

        preferred_payment = (
            tx.groupBy("customer_id", "payment_method")
            .agg(count("*").alias("pm_count"))
            .withColumn(
                "rn",
                row_number().over(
                    Window.partitionBy("customer_id").orderBy(col("pm_count").desc())
                ),
            )
            .filter(col("rn") == 1)
            .select("customer_id", col("payment_method").alias("preferred_payment_method"))
        )

        df = (
            customer_tx.join(preferred_category, "customer_id", "left")
            .join(preferred_payment, "customer_id", "left")
            .join(cst, "customer_id", "left")
            .withColumn(
                "risk_score",
                when(col("customer_lifetime_value") < 100, lit(0.80))
                .when(col("customer_lifetime_value") < 500, lit(0.50))
                .when(col("customer_lifetime_value") < 1000, lit(0.30))
                .otherwise(lit(0.10)),
            )
            .withColumn("_updated_at", current_timestamp())
            .withColumn("_load_date", lit(load_date))
        )

        row_count = df.count()
        df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(
            f"{GOLD}/customer_metrics"
        )
        logger.info(f"Gold customer_metrics: {row_count} rows")
        return row_count

    # Product Metrics

    def build_product_metrics(self, load_date: str) -> int:
        logger.info("Building gold_product_metrics")
        tx = (
            self.spark.read.format("delta")
            .load(f"{SILVER}/transactions")
            .filter(col("dq_is_valid"))
        )
        prd = self.spark.read.format("delta").load(f"{SILVER}/products")

        product_sales = tx.groupBy("product_id").agg(
            sum("quantity").alias("total_quantity_sold"),
            sum(col("amount")).alias("total_revenue"),
            avg(col("unit_price")).alias("avg_rating"),
            count("*").alias("txn_count"),
        )

        most_recent = tx.groupBy("product_id").agg(
            max(to_date("transaction_date")).alias("last_sale_date")
        )

        df = (
            prd.join(product_sales, "product_id", "left")
            .join(most_recent, "product_id", "left")
            .withColumn(
                "days_in_inventory",
                when(
                    col("last_sale_date").isNotNull(),
                    datediff(current_timestamp(), col("last_sale_date")),
                ).otherwise(lit(9999)),
            )
            .withColumn(
                "inventory_turnover_ratio",
                when(
                    col("stock_quantity") > 0,
                    round(col("total_quantity_sold") / col("stock_quantity"), 2),
                ).otherwise(lit(0)),
            )
            .withColumn(
                "is_profitable",
                when(
                    col("total_revenue") > col("price") * col("total_quantity_sold") * 0.7,
                    lit(True),
                ).otherwise(lit(False)),
            )
            .withColumn("_updated_at", current_timestamp())
            .withColumn("_load_date", lit(load_date))
        )

        row_count = df.count()
        df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(
            f"{GOLD}/product_metrics"
        )
        logger.info(f"Gold product_metrics: {row_count} rows")
        return row_count

    # Category Trends (moving avg, YoY)

    def build_category_trends(self, load_date: str) -> int:
        logger.info("Building gold_category_trends")
        tx = (
            self.spark.read.format("delta")
            .load(f"{SILVER}/transactions")
            .filter(col("dq_is_valid"))
        )
        prd = (
            self.spark.read.format("delta")
            .load(f"{SILVER}/products")
            .select("product_id", "category")
        )

        daily = (
            tx.join(prd, "product_id", "left")
            .withColumn("trend_date", to_date(col("transaction_date")))
            .groupBy("trend_date", "category")
            .agg(
                sum(col("amount")).alias("daily_sales"),
                sum("quantity").alias("daily_quantity"),
            )
        )

        window_7d = Window.partitionBy("category").orderBy("trend_date").rowsBetween(-6, 0)
        df = (
            daily.withColumn("moving_avg_7_days", round(avg("daily_sales").over(window_7d), 2))
            .withColumn("_updated_at", current_timestamp())
            .withColumn("_load_date", lit(load_date))
        )

        row_count = df.count()
        df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(
            f"{GOLD}/category_trends"
        )
        logger.info(f"Gold category_trends: {row_count} rows")
        return row_count


def main():
    spark = init_spark_session()
    agg = GoldLayerAggregator(spark)
    load_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Starting gold layer aggregation for {load_date}")

    try:
        results = {
            "daily_sales_fact": agg.build_daily_sales_fact(load_date),
            "customer_metrics": agg.build_customer_metrics(load_date),
            "product_metrics": agg.build_product_metrics(load_date),
            "category_trends": agg.build_category_trends(load_date),
        }

        for name in results:
            spark.read.format("delta").load(f"{GOLD}/{name}").createOrReplaceTempView(
                f"gold_{name}"
            )

        logger.info(f"Gold aggregation complete: {results}")
        spark.stop()

    except Exception as e:
        logger.error(f"Gold aggregation failed: {str(e)}")
        spark.stop()
        raise


if __name__ == "__main__":
    main()
