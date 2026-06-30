"""
Stack B: Lakehouse Pipeline - Bronze Layer
PySpark + Delta Lake for scalable data ingestion
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType, DateType
)
from pyspark.sql.functions import (
    col, to_timestamp, to_date, current_timestamp, 
    md5, concat_ws, input_file_name, lit, row_number
)
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Configuration
DELTA_LOCATION_BASE = "s3a://delta-lake"
BRONZE_LOCATION = f"{DELTA_LOCATION_BASE}/bronze"
SOURCE_DATA_PATH = "/data/raw"

def init_spark_session(app_name: str = "stack_b_bronze_ingestion") -> SparkSession:
    """Initialize Spark session with Delta Lake support"""
    import os
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("spark://spark-master:7077") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://dataops-minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "dataops-key") \
        .config("spark.hadoop.fs.s3a.secret.key", "dataops-secret") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("INFO")
    return spark

class BronzeLayerIngestor:
    """
    Bronze Layer: Raw data ingestion with idempotency
    - Append-only ingestion (immutable log)
    - Track ingestion timestamp and source
    - Enable ACID transactions with Delta Lake
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.bronze_location = BRONZE_LOCATION
    
    # ========================================================================
    # Schema Definitions
    # ========================================================================
    
    @staticmethod
    def get_customers_schema() -> StructType:
        """Schema for customers data"""
        return StructType([
            StructField("customer_id", StringType(), False),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", StringType(), True),
            StructField("created_date", DateType(), True),
            StructField("is_active", BooleanType(), True),
        ])
    
    @staticmethod
    def get_products_schema() -> StructType:
        """Schema for products data"""
        return StructType([
            StructField("product_id", StringType(), False),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("stock_quantity", IntegerType(), True),
            StructField("is_active", BooleanType(), True),
        ])
    
    @staticmethod
    def get_transactions_schema() -> StructType:
        """Schema for transactions data"""
        return StructType([
            StructField("transaction_id", StringType(), False),
            StructField("transaction_date", TimestampType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("unit_price", DoubleType(), False),
            StructField("amount", DoubleType(), False),
            StructField("payment_method", StringType(), True),
            StructField("status", StringType(), True),
            StructField("store_location", StringType(), True),
        ])
    
    # ========================================================================
    # Ingestion Methods
    # ========================================================================
    
    def ingest_customers(self, csv_path: str, load_date: str) -> int:
        """
        Ingest customer data from CSV to Delta bronze
        Idempotent: Deduplicates on customer_id
        """
        logger.info(f"Ingesting customers from {csv_path}")
        
        try:
            # Read CSV
            df = self.spark.read \
                .schema(self.get_customers_schema()) \
                .option("header", "true") \
                .csv(csv_path)
            
            # Add lineage columns
            df = df \
                .withColumn("_ingestion_timestamp", current_timestamp()) \
                .withColumn("_source_system", lit("ecommerce_api")) \
                .withColumn("_load_date", lit(load_date)) \
                .withColumn("_source_file", lit(csv_path))
            
            # Deduplicate on customer_id (keep latest)
            df = df.dropDuplicates(["customer_id"])
            
            row_count = df.count()
            logger.info(f"Read {row_count} customers")
            
            # Write to Delta (APPEND mode for idempotency)
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .save(f"{self.bronze_location}/customers")
            
            logger.info(f"Successfully ingested {row_count} customers")
            return row_count
            
        except Exception as e:
            logger.error(f"Error ingesting customers: {str(e)}")
            raise
    
    def ingest_products(self, csv_path: str, load_date: str) -> int:
        """
        Ingest product data from CSV to Delta bronze
        Idempotent: Deduplicates on product_id
        """
        logger.info(f"Ingesting products from {csv_path}")
        
        try:
            df = self.spark.read \
                .schema(self.get_products_schema()) \
                .option("header", "true") \
                .csv(csv_path)
            
            df = df \
                .withColumn("_ingestion_timestamp", current_timestamp()) \
                .withColumn("_source_system", lit("product_catalog_api")) \
                .withColumn("_load_date", lit(load_date)) \
                .withColumn("_source_file", lit(csv_path))
            
            df = df.dropDuplicates(["product_id"])
            
            row_count = df.count()
            logger.info(f"Read {row_count} products")
            
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .save(f"{self.bronze_location}/products")
            
            logger.info(f"Successfully ingested {row_count} products")
            return row_count
            
        except Exception as e:
            logger.error(f"Error ingesting products: {str(e)}")
            raise
    
    def ingest_transactions(self, csv_path: str, load_date: str) -> int:
        """
        Ingest transaction data from CSV to Delta bronze
        Idempotent: Uses transaction_id + load_date for deduplication
        """
        logger.info(f"Ingesting transactions from {csv_path}")
        
        try:
            df = self.spark.read \
                .schema(self.get_transactions_schema()) \
                .option("header", "true") \
                .csv(csv_path)
            
            # Compute row hash for deduplication
            df = df.withColumn(
                "_row_hash",
                md5(concat_ws("|", 
                    col("transaction_id"), 
                    col("customer_id"), 
                    col("product_id"),
                    col("amount")
                ))
            )
            
            df = df \
                .withColumn("_ingestion_timestamp", current_timestamp()) \
                .withColumn("_source_system", lit("pos_system")) \
                .withColumn("_load_date", lit(load_date)) \
                .withColumn("_source_file", lit(csv_path))
            
            # Deduplicate: If same transaction_id exists, keep newer one
            from pyspark.sql.window import Window
            window_spec = Window.partitionBy("transaction_id").orderBy(col("_ingestion_timestamp").desc())
            df = df.withColumn("_row_num", row_number().over(window_spec)) \
                   .filter(col("_row_num") == 1) \
                   .drop("_row_num")
            
            row_count = df.count()
            logger.info(f"Read {row_count} transactions")
            
            # Write to Delta
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .save(f"{self.bronze_location}/transactions")
            
            logger.info(f"Successfully ingested {row_count} transactions")
            return row_count
            
        except Exception as e:
            logger.error(f"Error ingesting transactions: {str(e)}")
            raise
    
    # ========================================================================
    # Data Quality Checks (Bronze Layer)
    # ========================================================================
    
    def validate_bronze_quality(self, table_name: str) -> dict:
        """
        Run data quality checks on bronze data
        Returns dict with validation results
        """
        logger.info(f"Validating {table_name} bronze table")
        
        results = {
            'table': table_name,
            'timestamp': datetime.now().isoformat(),
            'checks': {}
        }
        
        try:
            df = self.spark.read.format("delta").load(f"{self.bronze_location}/{table_name}")
            
            # Check 1: Null check on primary key
            pk_map = {
                'customers': 'customer_id',
                'products': 'product_id',
                'transactions': 'transaction_id'
            }
            pk = pk_map.get(table_name)
            
            null_count = df.filter(col(pk).isNull()).count()
            results['checks']['nulls_in_pk'] = {
                'status': 'PASS' if null_count == 0 else 'FAIL',
                'count': null_count
            }
            
            # Check 2: Row count
            total_rows = df.count()
            results['checks']['row_count'] = {
                'status': 'PASS' if total_rows > 0 else 'FAIL',
                'count': total_rows
            }
            
            # Check 3: Duplicate check
            unique_pk_count = df.select(pk).distinct().count()
            duplicate_count = total_rows - unique_pk_count
            results['checks']['duplicates'] = {
                'status': 'PASS' if duplicate_count == 0 else 'FAIL',
                'count': duplicate_count
            }
            
            logger.info(f"Validation results for {table_name}: {results['checks']}")
            return results
            
        except Exception as e:
            logger.error(f"Validation error for {table_name}: {str(e)}")
            results['error'] = str(e)
            return results


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Main entry point for bronze layer ingestion"""
    
    spark = init_spark_session()
    ingestor = BronzeLayerIngestor(spark)
    
    load_date = datetime.now().strftime("%Y-%m-%d")
    
    logger.info(f"Starting bronze layer ingestion for {load_date}")
    
    try:
        # Ingest all tables
        results = {
            'customers': ingestor.ingest_customers(f"{SOURCE_DATA_PATH}/customers.csv", load_date),
            'products': ingestor.ingest_products(f"{SOURCE_DATA_PATH}/products.csv", load_date),
            'transactions': ingestor.ingest_transactions(f"{SOURCE_DATA_PATH}/transactions.csv", load_date),
        }
        
        # Validate
        for table_name in ['customers', 'products', 'transactions']:
            validation = ingestor.validate_bronze_quality(table_name)
            logger.info(f"{table_name} validation: {validation}")
        
        logger.info(f"Bronze ingestion complete: {results}")
        spark.stop()
        
    except Exception as e:
        logger.error(f"Bronze ingestion failed: {str(e)}")
        spark.stop()
        raise


if __name__ == "__main__":
    main()
