"""
Stack B: Lakehouse Pipeline (PySpark + Delta Lake)
Medallion Architecture: Bronze -> Silver -> Gold
Orchestrated with Apache Airflow via SparkSubmitOperator
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
import logging

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'owner': 'dataops-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email': ['dataops@company.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

PYSPARK_APP_DIR = "/home/airflow/pyspark/stack_b"
SPARK_MASTER = "spark://spark-master:7077"
SPARK_CONN_ID = "spark_default"

SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.delta.logStore.class": "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
    "spark.hadoop.fs.s3a.endpoint": "http://dataops-minio:9000",
    "spark.hadoop.fs.s3a.access.key": "dataops-key",
    "spark.hadoop.fs.s3a.secret.key": "dataops-secret",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.jars.packages": (
        "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ),
    "spark.ui.enabled": "false",
}

with DAG(
    dag_id="ecommerce_lakehouse_stack_b_pipeline",
    default_args=DEFAULT_ARGS,
    description="Stack B: Lakehouse medallion pipeline (Bronze -> Silver -> Gold)",
    schedule_interval="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stack-b", "lakehouse", "pyspark", "delta"],
) as dag:

    with TaskGroup(group_id="bronze_layer") as bronze:
        bronze_ingestion = SparkSubmitOperator(
            task_id="bronze_ingestion",
            application=f"{PYSPARK_APP_DIR}/bronze_ingestion.py",
            conn_id=SPARK_CONN_ID,
            spark_binary_name="spark-submit",
            total_executor_cores=2,
            executor_memory="1g",
            driver_memory="1g",
            conf=SPARK_CONF,
            reconnect_on_retry=True,
            execution_timeout=timedelta(hours=1),
            retries=2,
        )

    with TaskGroup(group_id="silver_layer") as silver:
        silver_transformation = SparkSubmitOperator(
            task_id="silver_transformation",
            application=f"{PYSPARK_APP_DIR}/silver_transformation.py",
            conn_id=SPARK_CONN_ID,
            spark_binary_name="spark-submit",
            total_executor_cores=2,
            executor_memory="1g",
            driver_memory="1g",
            conf=SPARK_CONF,
            reconnect_on_retry=True,
            execution_timeout=timedelta(hours=1),
            retries=2,
        )

    with TaskGroup(group_id="gold_layer") as gold:
        gold_aggregation = SparkSubmitOperator(
            task_id="gold_aggregation",
            application=f"{PYSPARK_APP_DIR}/gold_aggregation.py",
            conn_id=SPARK_CONN_ID,
            spark_binary_name="spark-submit",
            total_executor_cores=2,
            executor_memory="1g",
            driver_memory="1g",
            conf=SPARK_CONF,
            reconnect_on_retry=True,
            execution_timeout=timedelta(hours=1),
            retries=2,
        )

    bronze >> silver >> gold
