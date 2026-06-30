"""
Stack B: Lakehouse Pipeline - Silver Layer
PySpark + Delta Lake: Cleaned, Validated, Deduplicated
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType, DateType
from pyspark.sql.functions import col, to_timestamp, to_date, current_timestamp, md5, concat_ws, lit, when, regexp_replace, sha2, row_number
from pyspark.sql.window import Window
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

DELTA_LOCATION_BASE = "/data/delta"
BRONZE_LOCATION = f"{DELTA_LOCATION_BASE}/bronze"
SILVER_LOCATION = f"{DELTA_LOCATION_BASE}/silver"


def init_spark_session(app_name: str = "stack_b_silver_transformation") -> SparkSession:
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    return spark


class SilverLayerTransformer:

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.bronze_location = BRONZE_LOCATION
        self.silver_location = SILVER_LOCATION

    # ========================================================================
    # Silver: Customers - PII Masking, Validation, Dedup
    # ========================================================================

    def transform_customers(self, load_date: str) -> int:
        logger.info("Transforming customers: bronze -> silver")
        df = self.spark.read.format("delta").load(f"{self.bronze_location}/customers")

        df_clean = df.dropDuplicates(["customer_id"])

        df_silver = df_clean.withColumn("email_masked", sha2(col("email"), 256)) \
            .withColumn("phone_masked", sha2(col("phone"), 256)) \
            .withColumn("dq_is_valid",
                        when(col("customer_id").isNull(), lit(False))
                        .when(col("first_name").isNull(), lit(False))
                        .when(col("email").isNull(), lit(False))
                        .otherwise(lit(True))) \
            .withColumn("dq_validation_errors",
                        when(col("customer_id").isNull(), lit("missing_customer_id"))
                        .when(col("first_name").isNull(), lit("missing_first_name"))
                        .when(col("email").isNull(), lit("missing_email"))
                        .otherwise(lit(None).cast(StringType()))) \
            .withColumn("_created_at", current_timestamp()) \
            .withColumn("_updated_at", current_timestamp()) \
            .withColumn("_load_date", lit(load_date))

        silver_cols = ["customer_id", "first_name", "last_name",
                       "email_masked", "phone_masked",
                       "city", "state", "zip_code", "created_date", "is_active",
                       "dq_is_valid", "dq_validation_errors",
                       "_ingestion_timestamp", "_source_system", "_load_date", "_source_file",
                       "_created_at", "_updated_at"]
        df_silver = df_silver.select([c for c in silver_cols if c in df_silver.columns])

        row_count = df_silver.count()
        df_silver.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(f"{self.silver_location}/customers")

        logger.info(f"Silver customers: {row_count} rows")
        return row_count

    # ========================================================================
    # Silver: Products - Validation, Dedup
    # ========================================================================

    def transform_products(self, load_date: str) -> int:
        logger.info("Transforming products: bronze -> silver")
        df = self.spark.read.format("delta").load(f"{self.bronze_location}/products")

        df_clean = df.dropDuplicates(["product_id"])

        df_silver = df_clean \
            .withColumn("dq_is_valid",
                        when(col("price").isNull() | (col("price") <= 0), lit(False))
                        .when(col("stock_quantity").isNull() | (col("stock_quantity") < 0), lit(False))
                        .otherwise(lit(True))) \
            .withColumn("dq_validation_errors",
                        when(col("price").isNull() | (col("price") <= 0), lit("invalid_price"))
                        .when(col("stock_quantity").isNull() | (col("stock_quantity") < 0), lit("invalid_stock"))
                        .otherwise(lit(None).cast(StringType()))) \
            .withColumn("_created_at", current_timestamp()) \
            .withColumn("_updated_at", current_timestamp()) \
            .withColumn("_load_date", lit(load_date))

        silver_cols = ["product_id", "product_name", "category", "price", "stock_quantity", "is_active",
                       "dq_is_valid", "dq_validation_errors",
                       "_ingestion_timestamp", "_source_system", "_load_date",
                       "_created_at", "_updated_at"]
        df_silver = df_silver.select([c for c in silver_cols if c in df_silver.columns])

        row_count = df_silver.count()
        df_silver.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(f"{self.silver_location}/products")

        logger.info(f"Silver products: {row_count} rows")
        return row_count

    # ========================================================================
    # Silver: Transactions - Validation, Dedup
    # ========================================================================

    def transform_transactions(self, load_date: str) -> int:
        logger.info("Transforming transactions: bronze -> silver")
        df = self.spark.read.format("delta").load(f"{self.bronze_location}/transactions")

        window_spec = Window.partitionBy("transaction_id").orderBy(col("_ingestion_timestamp").desc())
        df_deduped = df.withColumn("_rn", row_number().over(window_spec)) \
                       .filter(col("_rn") == 1) \
                       .drop("_rn", "_row_hash")

        df_silver = df_deduped \
            .withColumn("dq_is_valid",
                        when(col("transaction_id").isNull(), lit(False))
                        .when(col("quantity").isNull() | (col("quantity") <= 0), lit(False))
                        .when(col("unit_price").isNull() | (col("unit_price") <= 0), lit(False))
                        .when(col("amount").isNull() | (col("amount") <= 0), lit(False))
                        .otherwise(lit(True))) \
            .withColumn("dq_validation_errors",
                        when(col("transaction_id").isNull(), lit("missing_transaction_id"))
                        .when(col("quantity").isNull() | (col("quantity") <= 0), lit("invalid_quantity"))
                        .when(col("unit_price").isNull() | (col("unit_price") <= 0), lit("invalid_unit_price"))
                        .when(col("amount").isNull() | (col("amount") <= 0), lit("invalid_amount"))
                        .otherwise(lit(None).cast(StringType()))) \
            .withColumn("dq_duplicate_found", lit(False)) \
            .withColumn("_created_at", current_timestamp()) \
            .withColumn("_updated_at", current_timestamp()) \
            .withColumn("_load_date", lit(load_date))

        silver_cols = ["transaction_id", "transaction_date", "customer_id", "product_id",
                       "quantity", "unit_price", "amount", "payment_method", "status", "store_location",
                       "dq_is_valid", "dq_validation_errors", "dq_duplicate_found",
                       "_ingestion_timestamp", "_source_system", "_load_date", "_source_file",
                       "_created_at", "_updated_at"]
        df_silver = df_silver.select([c for c in silver_cols if c in df_silver.columns])

        row_count = df_silver.count()
        df_silver.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(f"{self.silver_location}/transactions")

        logger.info(f"Silver transactions: {row_count} rows")
        return row_count


def main():
    spark = init_spark_session()
    transformer = SilverLayerTransformer(spark)
    load_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Starting silver layer transformation for {load_date}")

    try:
        results = {
            'customers': transformer.transform_customers(load_date),
            'products': transformer.transform_products(load_date),
            'transactions': transformer.transform_transactions(load_date),
        }

        spark.read.format("delta").load(f"{SILVER_LOCATION}/customers").createOrReplaceTempView("silver_customers")
        spark.read.format("delta").load(f"{SILVER_LOCATION}/products").createOrReplaceTempView("silver_products")
        spark.read.format("delta").load(f"{SILVER_LOCATION}/transactions").createOrReplaceTempView("silver_transactions")

        logger.info(f"Silver transformation complete: {results}")
        spark.stop()

    except Exception as e:
        logger.error(f"Silver transformation failed: {str(e)}")
        spark.stop()
        raise


if __name__ == "__main__":
    main()
