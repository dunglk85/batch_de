You are the Edge Case Hunter. Please review the following diff. You have read access to the project. Invoke via the bmad-review-edge-case-hunter skill.

diff --git a/pyspark/stack_b/bronze_ingestion.py b/pyspark/stack_b/bronze_ingestion.py
new file mode 100644
index 0000000..95ec8e5
--- /dev/null
+++ b/pyspark/stack_b/bronze_ingestion.py
@@ -0,0 +1,338 @@
+"""
+Stack B: Lakehouse Pipeline - Bronze Layer
+PySpark + Delta Lake for scalable data ingestion
+"""
+
+from pyspark.sql import SparkSession, DataFrame
+from pyspark.sql.types import (
+    StructType, StructField, StringType, IntegerType, 
+    DoubleType, BooleanType, TimestampType, DateType
+)
+from pyspark.sql.functions import (
+    col, to_timestamp, to_date, current_timestamp, 
+    md5, concat_ws, input_file_name, lit, row_number
+)
+from datetime import datetime
+import logging
+
+logger = logging.getLogger(__name__)
+
+# Configuration
+DELTA_LOCATION_BASE = "/data/delta"
+BRONZE_LOCATION = f"{DELTA_LOCATION_BASE}/bronze"
+SOURCE_DATA_PATH = "/data/raw"
+
+def init_spark_session(app_name: str = "stack_b_bronze_ingestion") -> SparkSession:
+    """Initialize Spark session with Delta Lake support"""
+    import os
+    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
+    spark = SparkSession.builder \
+        .appName(app_name) \
+        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
+        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
+        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
+        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
+        .config("spark.ui.enabled", "false") \
+        .getOrCreate()
+    
+    spark.sparkContext.setLogLevel("INFO")
+    return spark
+
+class BronzeLayerIngestor:
+    """
+    Bronze Layer: Raw data ingestion with idempotency
+    - Append-only ingestion (immutable log)
+    - Track ingestion timestamp and source
+    - Enable ACID transactions with Delta Lake
+    """
+    
+    def __init__(self, spark: SparkSession):
+        self.spark = spark
+        self.bronze_location = BRONZE_LOCATION
+    
+    # ========================================================================
+    # Schema Definitions
+    # ========================================================================
+    
+    @staticmethod
+    def get_customers_schema() -> StructType:
+        """Schema for customers data"""
+        return StructType([
+            StructField("customer_id", StringType(), False),
+            StructField("first_name", StringType(), True),
+            StructField("last_name", StringType(), True),
+            StructField("email", StringType(), True),
+            StructField("phone", StringType(), True),
+            StructField("city", StringType(), True),
+            StructField("state", StringType(), True),
+            StructField("zip_code", StringType(), True),
+            StructField("created_date", DateType(), True),
+            StructField("is_active", BooleanType(), True),
+        ])
+    
+    @staticmethod
+    def get_products_schema() -> StructType:
+        """Schema for products data"""
+        return StructType([
+            StructField("product_id", StringType(), False),
+            StructField("product_name", StringType(), True),
+            StructField("category", StringType(), True),
+            StructField("price", DoubleType(), True),
+            StructField("stock_quantity", IntegerType(), True),
+            StructField("is_active", BooleanType(), True),
+        ])
+    
+    @staticmethod
+    def get_transactions_schema() -> StructType:
+        """Schema for transactions data"""
+        return StructType([
+            StructField("transaction_id", StringType(), False),
+            StructField("transaction_date", TimestampType(), False),
+            StructField("customer_id", StringType(), False),
+            StructField("product_id", StringType(), False),
+            StructField("quantity", IntegerType(), False),
+            StructField("unit_price", DoubleType(), False),
+            StructField("amount", DoubleType(), False),
+            StructField("payment_method", StringType(), True),
+            StructField("status", StringType(), True),
+            StructField("store_location", StringType(), True),
+        ])
+    
+    # ========================================================================
+    # Ingestion Methods
+    # ========================================================================
+    
+    def ingest_customers(self, csv_path: str, load_date: str) -> int:
+        """
+        Ingest customer data from CSV to Delta bronze
+        Idempotent: Deduplicates on customer_id
+        """
+        logger.info(f"Ingesting customers from {csv_path}")
+        
+        try:
+            # Read CSV
+            df = self.spark.read \
+                .schema(self.get_customers_schema()) \
+                .option("header", "true") \
+                .csv(csv_path)
+            
+            # Add lineage columns
+            df = df \
+                .withColumn("_ingestion_timestamp", current_timestamp()) \
+                .withColumn("_source_system", lit("ecommerce_api")) \
+                .withColumn("_load_date", lit(load_date)) \
+                .withColumn("_source_file", lit(csv_path))
+            
+            # Deduplicate on customer_id (keep latest)
+            df = df.dropDuplicates(["customer_id"])
+            
+            row_count = df.count()
+            logger.info(f"Read {row_count} customers")
+            
+            # Write to Delta (APPEND mode for idempotency)
+            df.write \
+                .format("delta") \
+                .mode("append") \
+                .option("mergeSchema", "true") \
+                .save(f"{self.bronze_location}/customers")
+            
+            logger.info(f"Successfully ingested {row_count} customers")
+            return row_count
+            
+        except Exception as e:
+            logger.error(f"Error ingesting customers: {str(e)}")
+            raise
+    
+    def ingest_products(self, csv_path: str, load_date: str) -> int:
+        """
+        Ingest product data from CSV to Delta bronze
+        Idempotent: Deduplicates on product_id
+        """
+        logger.info(f"Ingesting products from {csv_path}")
+        
+        try:
+            df = self.spark.read \
+                .schema(self.get_products_schema()) \
+                .option("header", "true") \
+                .csv(csv_path)
+            
+            df = df \
+                .withColumn("_ingestion_timestamp", current_timestamp()) \
+                .withColumn("_source_system", lit("product_catalog_api")) \
+                .withColumn("_load_date", lit(load_date)) \
+                .withColumn("_source_file", lit(csv_path))
+            
+            df = df.dropDuplicates(["product_id"])
+            
+            row_count = df.count()
+            logger.info(f"Read {row_count} products")
+            
+            df.write \
+                .format("delta") \
+                .mode("append") \
+                .option("mergeSchema", "true") \
+                .save(f"{self.bronze_location}/products")
+            
+            logger.info(f"Successfully ingested {row_count} products")
+            return row_count
+            
+        except Exception as e:
+            logger.error(f"Error ingesting products: {str(e)}")
+            raise
+    
+    def ingest_transactions(self, csv_path: str, load_date: str) -> int:
+        """
+        Ingest transaction data from CSV to Delta bronze
+        Idempotent: Uses transaction_id + load_date for deduplication
+        """
+        logger.info(f"Ingesting transactions from {csv_path}")
+        
+        try:
+            df = self.spark.read \
+                .schema(self.get_transactions_schema()) \
+                .option("header", "true") \
+                .csv(csv_path)
+            
+            # Compute row hash for deduplication
+            df = df.withColumn(
+                "_row_hash",
+                md5(concat_ws("|", 
+                    col("transaction_id"), 
+                    col("customer_id"), 
+                    col("product_id"),
+                    col("amount")
+                ))
+            )
+            
+            df = df \
+                .withColumn("_ingestion_timestamp", current_timestamp()) \
+                .withColumn("_source_system", lit("pos_system")) \
+                .withColumn("_load_date", lit(load_date)) \
+                .withColumn("_source_file", lit(csv_path))
+            
+            # Deduplicate: If same transaction_id exists, keep newer one
+            from pyspark.sql.window import Window
+            window_spec = Window.partitionBy("transaction_id").orderBy(col("_ingestion_timestamp").desc())
+            df = df.withColumn("_row_num", row_number().over(window_spec)) \
+                   .filter(col("_row_num") == 1) \
+                   .drop("_row_num")
+            
+            row_count = df.count()
+            logger.info(f"Read {row_count} transactions")
+            
+            # Write to Delta
+            df.write \
+                .format("delta") \
+                .mode("append") \
+                .option("mergeSchema", "true") \
+                .save(f"{self.bronze_location}/transactions")
+            
+            logger.info(f"Successfully ingested {row_count} transactions")
+            return row_count
+            
+        except Exception as e:
+            logger.error(f"Error ingesting transactions: {str(e)}")
+            raise
+    
+    # ========================================================================
+    # Data Quality Checks (Bronze Layer)
+    # ========================================================================
+    
+    def validate_bronze_quality(self, table_name: str) -> dict:
+        """
+        Run data quality checks on bronze data
+        Returns dict with validation results
+        """
+        logger.info(f"Validating {table_name} bronze table")
+        
+        results = {
+            'table': table_name,
+            'timestamp': datetime.now().isoformat(),
+            'checks': {}
+        }
+        
+        try:
+            df = self.spark.read.format("delta").load(f"{self.bronze_location}/{table_name}")
+            
+            # Check 1: Null check on primary key
+            pk_map = {
+                'customers': 'customer_id',
+                'products': 'product_id',
+                'transactions': 'transaction_id'
+            }
+            pk = pk_map.get(table_name)
+            
+            null_count = df.filter(col(pk).isNull()).count()
+            results['checks']['nulls_in_pk'] = {
+                'status': 'PASS' if null_count == 0 else 'FAIL',
+                'count': null_count
+            }
+            
+            # Check 2: Row count
+            total_rows = df.count()
+            results['checks']['row_count'] = {
+                'status': 'PASS' if total_rows > 0 else 'FAIL',
+                'count': total_rows
+            }
+            
+            # Check 3: Duplicate check
+            unique_pk_count = df.select(pk).distinct().count()
+            duplicate_count = total_rows - unique_pk_count
+            results['checks']['duplicates'] = {
+                'status': 'PASS' if duplicate_count == 0 else 'FAIL',
+                'count': duplicate_count
+            }
+            
+            logger.info(f"Validation results for {table_name}: {results['checks']}")
+            return results
+            
+        except Exception as e:
+            logger.error(f"Validation error for {table_name}: {str(e)}")
+            results['error'] = str(e)
+            return results
+
+
+# ============================================================================
+# Main Execution
+# ============================================================================
+
+def main():
+    """Main entry point for bronze layer ingestion"""
+    
+    spark = init_spark_session()
+    ingestor = BronzeLayerIngestor(spark)
+    
+    load_date = datetime.now().strftime("%Y-%m-%d")
+    
+    logger.info(f"Starting bronze layer ingestion for {load_date}")
+    
+    try:
+        # Ingest all tables
+        results = {
+            'customers': ingestor.ingest_customers(f"{SOURCE_DATA_PATH}/customers.csv", load_date),
+            'products': ingestor.ingest_products(f"{SOURCE_DATA_PATH}/products.csv", load_date),
+            'transactions': ingestor.ingest_transactions(f"{SOURCE_DATA_PATH}/transactions.csv", load_date),
+        }
+        
+        # Validate
+        for table_name in ['customers', 'products', 'transactions']:
+            validation = ingestor.validate_bronze_quality(table_name)
+            logger.info(f"{table_name} validation: {validation}")
+        
+        logger.info(f"Bronze ingestion complete: {results}")
+        
+        # Create or replace views for easy SQL access
+        spark.read.format("delta").load(f"{DELTA_LOCATION_BASE}/bronze/customers").createOrReplaceTempView("bronze_customers")
+        spark.read.format("delta").load(f"{DELTA_LOCATION_BASE}/bronze/products").createOrReplaceTempView("bronze_products")
+        spark.read.format("delta").load(f"{DELTA_LOCATION_BASE}/bronze/transactions").createOrReplaceTempView("bronze_transactions")
+        
+        spark.stop()
+        
+    except Exception as e:
+        logger.error(f"Bronze ingestion failed: {str(e)}")
+        spark.stop()
+        raise
+
+
+if __name__ == "__main__":
+    main()
diff --git a/pyspark/stack_b/gold_aggregation.py b/pyspark/stack_b/gold_aggregation.py
new file mode 100644
index 0000000..964f1a9
--- /dev/null
+++ b/pyspark/stack_b/gold_aggregation.py
@@ -0,0 +1,228 @@
+"""
+Stack B: Lakehouse Pipeline - Gold Layer
+PySpark + Delta Lake: Analytics-ready aggregates and KPIs
+"""
+
+from pyspark.sql import SparkSession, DataFrame
+from pyspark.sql.functions import col, to_date, current_timestamp, count, sum, avg, when, lit, min, max, round, datediff, first, last, row_number
+from pyspark.sql.window import Window
+from datetime import datetime
+import logging
+
+logger = logging.getLogger(__name__)
+
+DELTA_BASE = "/data/delta"
+BRONZE = f"{DELTA_BASE}/bronze"
+SILVER = f"{DELTA_BASE}/silver"
+GOLD = f"{DELTA_BASE}/gold"
+
+
+def init_spark_session(app_name: str = "stack_b_gold_aggregation") -> SparkSession:
+    import os
+    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
+    spark = SparkSession.builder \
+        .appName(app_name) \
+        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
+        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
+        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
+        .config("spark.ui.enabled", "false") \
+        .getOrCreate()
+    spark.sparkContext.setLogLevel("INFO")
+    return spark
+
+
+class GoldLayerAggregator:
+
+    def __init__(self, spark: SparkSession):
+        self.spark = spark
+
+    # ========================================================================
+    # Daily Sales Fact
+    # ========================================================================
+
+    def build_daily_sales_fact(self, load_date: str) -> int:
+        logger.info("Building gold_daily_sales_fact")
+        tx = self.spark.read.format("delta").load(f"{SILVER}/transactions") \
+            .filter(col("dq_is_valid") == True)
+        prd = self.spark.read.format("delta").load(f"{SILVER}/products") \
+            .select("product_id", "category")
+
+        df = tx.join(prd, "product_id", "left") \
+            .withColumn("txn_date", to_date(col("transaction_date"))) \
+            .groupBy("txn_date", "customer_id", "product_id", "category", "store_location") \
+            .agg(
+                sum("quantity").alias("quantity"),
+                sum(when(col("status") == "completed", col("amount")).otherwise(lit(0))).alias("gross_amount"),
+                sum(col("amount")).alias("net_amount"),
+                count("*").alias("transaction_count"),
+                count(when(col("status") == "completed", 1)).alias("completed_count"),
+                count(when(col("status") == "refunded", 1)).alias("refunded_count"),
+            ) \
+            .withColumn("_updated_at", current_timestamp()) \
+            .withColumn("_load_date", lit(load_date))
+
+        row_count = df.count()
+        df.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
+            .save(f"{GOLD}/daily_sales_fact")
+        logger.info(f"Gold daily_sales_fact: {row_count} rows")
+        return row_count
+
+    # ========================================================================
+    # Customer Metrics
+    # ========================================================================
+
+    def build_customer_metrics(self, load_date: str) -> int:
+        logger.info("Building gold_customer_metrics")
+        tx = self.spark.read.format("delta").load(f"{SILVER}/transactions") \
+            .filter(col("dq_is_valid") == True)
+        cst = self.spark.read.format("delta").load(f"{SILVER}/customers") \
+            .select("customer_id", "is_active")
+        prd = self.spark.read.format("delta").load(f"{SILVER}/products") \
+            .select("product_id", "category")
+
+        tx_with_cat = tx.join(prd, "product_id", "left")
+
+        customer_tx = tx.groupBy("customer_id") \
+            .agg(
+                sum("amount").alias("customer_lifetime_value"),
+                count("*").alias("total_transactions"),
+                min(to_date("transaction_date")).alias("first_purchase_date"),
+                max(to_date("transaction_date")).alias("last_purchase_date"),
+                avg("amount").alias("avg_transaction_amount"),
+            )
+
+        preferred_category = tx_with_cat.groupBy("customer_id", "category") \
+            .agg(count("*").alias("cat_count")) \
+            .withColumn("rn", row_number().over(Window.partitionBy("customer_id").orderBy(col("cat_count").desc()))) \
+            .filter(col("rn") == 1) \
+            .select("customer_id", col("category").alias("preferred_category"))
+
+        preferred_payment = tx.groupBy("customer_id", "payment_method") \
+            .agg(count("*").alias("pm_count")) \
+            .withColumn("rn", row_number().over(Window.partitionBy("customer_id").orderBy(col("pm_count").desc()))) \
+            .filter(col("rn") == 1) \
+            .select("customer_id", col("payment_method").alias("preferred_payment_method"))
+
+        df = customer_tx \
+            .join(preferred_category, "customer_id", "left") \
+            .join(preferred_payment, "customer_id", "left") \
+            .join(cst, "customer_id", "left") \
+            .withColumn("risk_score",
+                        when(col("customer_lifetime_value") < 100, lit(0.80))
+                        .when(col("customer_lifetime_value") < 500, lit(0.50))
+                        .when(col("customer_lifetime_value") < 1000, lit(0.30))
+                        .otherwise(lit(0.10))) \
+            .withColumn("_updated_at", current_timestamp()) \
+            .withColumn("_load_date", lit(load_date))
+
+        row_count = df.count()
+        df.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
+            .save(f"{GOLD}/customer_metrics")
+        logger.info(f"Gold customer_metrics: {row_count} rows")
+        return row_count
+
+    # ========================================================================
+    # Product Metrics
+    # ========================================================================
+
+    def build_product_metrics(self, load_date: str) -> int:
+        logger.info("Building gold_product_metrics")
+        tx = self.spark.read.format("delta").load(f"{SILVER}/transactions") \
+            .filter(col("dq_is_valid") == True)
+        prd = self.spark.read.format("delta").load(f"{SILVER}/products")
+
+        product_sales = tx.groupBy("product_id") \
+            .agg(
+                sum("quantity").alias("total_quantity_sold"),
+                sum(col("amount")).alias("total_revenue"),
+                avg(col("unit_price")).alias("avg_rating"),
+                count("*").alias("txn_count"),
+            )
+
+        most_recent = tx.groupBy("product_id") \
+            .agg(max(to_date("transaction_date")).alias("last_sale_date"))
+
+        df = prd \
+            .join(product_sales, "product_id", "left") \
+            .join(most_recent, "product_id", "left") \
+            .withColumn("days_in_inventory",
+                        when(col("last_sale_date").isNotNull(),
+                             datediff(current_timestamp(), col("last_sale_date")))
+                        .otherwise(lit(9999))) \
+            .withColumn("inventory_turnover_ratio",
+                        when(col("stock_quantity") > 0,
+                             round(col("total_quantity_sold") / col("stock_quantity"), 2))
+                        .otherwise(lit(0))) \
+            .withColumn("is_profitable",
+                        when(col("total_revenue") > col("price") * col("total_quantity_sold") * 0.7, lit(True))
+                        .otherwise(lit(False))) \
+            .withColumn("_updated_at", current_timestamp()) \
+            .withColumn("_load_date", lit(load_date))
+
+        row_count = df.count()
+        df.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
+            .save(f"{GOLD}/product_metrics")
+        logger.info(f"Gold product_metrics: {row_count} rows")
+        return row_count
+
+    # ========================================================================
+    # Category Trends (moving avg, YoY)
+    # ========================================================================
+
+    def build_category_trends(self, load_date: str) -> int:
+        logger.info("Building gold_category_trends")
+        tx = self.spark.read.format("delta").load(f"{SILVER}/transactions") \
+            .filter(col("dq_is_valid") == True)
+        prd = self.spark.read.format("delta").load(f"{SILVER}/products") \
+            .select("product_id", "category")
+
+        daily = tx.join(prd, "product_id", "left") \
+            .withColumn("trend_date", to_date(col("transaction_date"))) \
+            .groupBy("trend_date", "category") \
+            .agg(
+                sum(col("amount")).alias("daily_sales"),
+                sum("quantity").alias("daily_quantity"),
+            )
+
+        window_7d = Window.partitionBy("category").orderBy("trend_date").rowsBetween(-6, 0)
+        df = daily \
+            .withColumn("moving_avg_7_days", round(avg("daily_sales").over(window_7d), 2)) \
+            .withColumn("_updated_at", current_timestamp()) \
+            .withColumn("_load_date", lit(load_date))
+
+        row_count = df.count()
+        df.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
+            .save(f"{GOLD}/category_trends")
+        logger.info(f"Gold category_trends: {row_count} rows")
+        return row_count
+
+
+def main():
+    spark = init_spark_session()
+    agg = GoldLayerAggregator(spark)
+    load_date = datetime.now().strftime("%Y-%m-%d")
+
+    logger.info(f"Starting gold layer aggregation for {load_date}")
+
+    try:
+        results = {
+            'daily_sales_fact': agg.build_daily_sales_fact(load_date),
+            'customer_metrics': agg.build_customer_metrics(load_date),
+            'product_metrics': agg.build_product_metrics(load_date),
+            'category_trends': agg.build_category_trends(load_date),
+        }
+
+        for name in results:
+            spark.read.format("delta").load(f"{GOLD}/{name}").createOrReplaceTempView(f"gold_{name}")
+
+        logger.info(f"Gold aggregation complete: {results}")
+        spark.stop()
+
+    except Exception as e:
+        logger.error(f"Gold aggregation failed: {str(e)}")
+        spark.stop()
+        raise
+
+
+if __name__ == "__main__":
+    main()
diff --git a/pyspark/stack_b/silver_transformation.py b/pyspark/stack_b/silver_transformation.py
new file mode 100644
index 0000000..bba75bb
--- /dev/null
+++ b/pyspark/stack_b/silver_transformation.py
@@ -0,0 +1,201 @@
+"""
+Stack B: Lakehouse Pipeline - Silver Layer
+PySpark + Delta Lake: Cleaned, Validated, Deduplicated
+"""
+
+from pyspark.sql import SparkSession, DataFrame
+from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType, DateType
+from pyspark.sql.functions import col, to_timestamp, to_date, current_timestamp, md5, concat_ws, lit, when, regexp_replace, sha2, row_number
+from pyspark.sql.window import Window
+from datetime import datetime
+import logging
+
+logger = logging.getLogger(__name__)
+
+DELTA_LOCATION_BASE = "/data/delta"
+BRONZE_LOCATION = f"{DELTA_LOCATION_BASE}/bronze"
+SILVER_LOCATION = f"{DELTA_LOCATION_BASE}/silver"
+
+
+def init_spark_session(app_name: str = "stack_b_silver_transformation") -> SparkSession:
+    import os
+    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
+    spark = SparkSession.builder \
+        .appName(app_name) \
+        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
+        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
+        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
+        .config("spark.ui.enabled", "false") \
+        .getOrCreate()
+    spark.sparkContext.setLogLevel("INFO")
+    return spark
+
+
+class SilverLayerTransformer:
+
+    def __init__(self, spark: SparkSession):
+        self.spark = spark
+        self.bronze_location = BRONZE_LOCATION
+        self.silver_location = SILVER_LOCATION
+
+    # ========================================================================
+    # Silver: Customers - PII Masking, Validation, Dedup
+    # ========================================================================
+
+    def transform_customers(self, load_date: str) -> int:
+        logger.info("Transforming customers: bronze -> silver")
+        df = self.spark.read.format("delta").load(f"{self.bronze_location}/customers")
+
+        df_clean = df.dropDuplicates(["customer_id"])
+
+        df_silver = df_clean.withColumn("email_masked", sha2(col("email"), 256)) \
+            .withColumn("phone_masked", sha2(col("phone"), 256)) \
+            .withColumn("dq_is_valid",
+                        when(col("customer_id").isNull(), lit(False))
+                        .when(col("first_name").isNull(), lit(False))
+                        .when(col("email").isNull(), lit(False))
+                        .otherwise(lit(True))) \
+            .withColumn("dq_validation_errors",
+                        when(col("customer_id").isNull(), lit("missing_customer_id"))
+                        .when(col("first_name").isNull(), lit("missing_first_name"))
+                        .when(col("email").isNull(), lit("missing_email"))
+                        .otherwise(lit(None).cast(StringType()))) \
+            .withColumn("_created_at", current_timestamp()) \
+            .withColumn("_updated_at", current_timestamp()) \
+            .withColumn("_load_date", lit(load_date))
+
+        silver_cols = ["customer_id", "first_name", "last_name",
+                       "email_masked", "phone_masked",
+                       "city", "state", "zip_code", "created_date", "is_active",
+                       "dq_is_valid", "dq_validation_errors",
+                       "_ingestion_timestamp", "_source_system", "_load_date", "_source_file",
+                       "_created_at", "_updated_at"]
+        df_silver = df_silver.select([c for c in silver_cols if c in df_silver.columns])
+
+        row_count = df_silver.count()
+        df_silver.write \
+            .format("delta") \
+            .mode("overwrite") \
+            .option("mergeSchema", "true") \
+            .save(f"{self.silver_location}/customers")
+
+        logger.info(f"Silver customers: {row_count} rows")
+        return row_count
+
+    # ========================================================================
+    # Silver: Products - Validation, Dedup
+    # ========================================================================
+
+    def transform_products(self, load_date: str) -> int:
+        logger.info("Transforming products: bronze -> silver")
+        df = self.spark.read.format("delta").load(f"{self.bronze_location}/products")
+
+        df_clean = df.dropDuplicates(["product_id"])
+
+        df_silver = df_clean \
+            .withColumn("dq_is_valid",
+                        when(col("price").isNull() | (col("price") <= 0), lit(False))
+                        .when(col("stock_quantity").isNull() | (col("stock_quantity") < 0), lit(False))
+                        .otherwise(lit(True))) \
+            .withColumn("dq_validation_errors",
+                        when(col("price").isNull() | (col("price") <= 0), lit("invalid_price"))
+                        .when(col("stock_quantity").isNull() | (col("stock_quantity") < 0), lit("invalid_stock"))
+                        .otherwise(lit(None).cast(StringType()))) \
+            .withColumn("_created_at", current_timestamp()) \
+            .withColumn("_updated_at", current_timestamp()) \
+            .withColumn("_load_date", lit(load_date))
+
+        silver_cols = ["product_id", "product_name", "category", "price", "stock_quantity", "is_active",
+                       "dq_is_valid", "dq_validation_errors",
+                       "_ingestion_timestamp", "_source_system", "_load_date",
+                       "_created_at", "_updated_at"]
+        df_silver = df_silver.select([c for c in silver_cols if c in df_silver.columns])
+
+        row_count = df_silver.count()
+        df_silver.write \
+            .format("delta") \
+            .mode("overwrite") \
+            .option("mergeSchema", "true") \
+            .save(f"{self.silver_location}/products")
+
+        logger.info(f"Silver products: {row_count} rows")
+        return row_count
+
+    # ========================================================================
+    # Silver: Transactions - Validation, Dedup
+    # ========================================================================
+
+    def transform_transactions(self, load_date: str) -> int:
+        logger.info("Transforming transactions: bronze -> silver")
+        df = self.spark.read.format("delta").load(f"{self.bronze_location}/transactions")
+
+        window_spec = Window.partitionBy("transaction_id").orderBy(col("_ingestion_timestamp").desc())
+        df_deduped = df.withColumn("_rn", row_number().over(window_spec)) \
+                       .filter(col("_rn") == 1) \
+                       .drop("_rn", "_row_hash")
+
+        df_silver = df_deduped \
+            .withColumn("dq_is_valid",
+                        when(col("transaction_id").isNull(), lit(False))
+                        .when(col("quantity").isNull() | (col("quantity") <= 0), lit(False))
+                        .when(col("unit_price").isNull() | (col("unit_price") <= 0), lit(False))
+                        .when(col("amount").isNull() | (col("amount") <= 0), lit(False))
+                        .otherwise(lit(True))) \
+            .withColumn("dq_validation_errors",
+                        when(col("transaction_id").isNull(), lit("missing_transaction_id"))
+                        .when(col("quantity").isNull() | (col("quantity") <= 0), lit("invalid_quantity"))
+                        .when(col("unit_price").isNull() | (col("unit_price") <= 0), lit("invalid_unit_price"))
+                        .when(col("amount").isNull() | (col("amount") <= 0), lit("invalid_amount"))
+                        .otherwise(lit(None).cast(StringType()))) \
+            .withColumn("dq_duplicate_found", lit(False)) \
+            .withColumn("_created_at", current_timestamp()) \
+            .withColumn("_updated_at", current_timestamp()) \
+            .withColumn("_load_date", lit(load_date))
+
+        silver_cols = ["transaction_id", "transaction_date", "customer_id", "product_id",
+                       "quantity", "unit_price", "amount", "payment_method", "status", "store_location",
+                       "dq_is_valid", "dq_validation_errors", "dq_duplicate_found",
+                       "_ingestion_timestamp", "_source_system", "_load_date", "_source_file",
+                       "_created_at", "_updated_at"]
+        df_silver = df_silver.select([c for c in silver_cols if c in df_silver.columns])
+
+        row_count = df_silver.count()
+        df_silver.write \
+            .format("delta") \
+            .mode("overwrite") \
+            .option("mergeSchema", "true") \
+            .save(f"{self.silver_location}/transactions")
+
+        logger.info(f"Silver transactions: {row_count} rows")
+        return row_count
+
+
+def main():
+    spark = init_spark_session()
+    transformer = SilverLayerTransformer(spark)
+    load_date = datetime.now().strftime("%Y-%m-%d")
+
+    logger.info(f"Starting silver layer transformation for {load_date}")
+
+    try:
+        results = {
+            'customers': transformer.transform_customers(load_date),
+            'products': transformer.transform_products(load_date),
+            'transactions': transformer.transform_transactions(load_date),
+        }
+
+        spark.read.format("delta").load(f"{SILVER_LOCATION}/customers").createOrReplaceTempView("silver_customers")
+        spark.read.format("delta").load(f"{SILVER_LOCATION}/products").createOrReplaceTempView("silver_products")
+        spark.read.format("delta").load(f"{SILVER_LOCATION}/transactions").createOrReplaceTempView("silver_transactions")
+
+        logger.info(f"Silver transformation complete: {results}")
+        spark.stop()
+
+    except Exception as e:
+        logger.error(f"Silver transformation failed: {str(e)}")
+        spark.stop()
+        raise
+
+
+if __name__ == "__main__":
+    main()
diff --git a/pyspark/test_airflow_spark.py b/pyspark/test_airflow_spark.py
new file mode 100644
index 0000000..cc37557
--- /dev/null
+++ b/pyspark/test_airflow_spark.py
@@ -0,0 +1,6 @@
+import os
+os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
+from pyspark.sql import SparkSession
+s = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
+print("Spark OK:", s.version)
+s.stop()

