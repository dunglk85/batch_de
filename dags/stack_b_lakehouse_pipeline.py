"""
Stack B: Lakehouse Pipeline (PySpark + Delta Lake)
Medallion Architecture: Bronze -> Silver -> Gold
Orchestrated with Apache Airflow via spark-submit
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
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

SPARK_MASTER = "spark://spark-master:7077"
PYSPARK_DIR = "/opt/spark-apps/stack_b"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
PACKAGES = "io.delta:delta-spark_2.12:3.0.0"
SPARK_CONF = "--conf spark.jars.ivy=/tmp/ivy"
SPARK_MEM = "--executor-memory 1g --driver-memory 1g"


def build_spark_cmd(script_name: str) -> str:
    return (f"{SPARK_SUBMIT} --master {SPARK_MASTER} "
            f"--packages {PACKAGES} {SPARK_CONF} {SPARK_MEM} "
            f"{PYSPARK_DIR}/{script_name}")


with DAG(
    dag_id="ecommerce_lakehouse_stack_b_pipeline",
    default_args=DEFAULT_ARGS,
    description="Stack B: Lakehouse medallion pipeline (Bronze -> Silver -> Gold)",
    schedule_interval="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["stack-b", "lakehouse", "pyspark", "delta"],
) as dag:

    with TaskGroup(group_id="bronze_layer") as bronze:
        bronze_ingestion = BashOperator(
            task_id="bronze_ingestion",
            bash_command=build_spark_cmd("bronze_ingestion.py"),
            retries=2,
        )

    with TaskGroup(group_id="silver_layer") as silver:
        silver_transformation = BashOperator(
            task_id="silver_transformation",
            bash_command=build_spark_cmd("silver_transformation.py"),
            retries=2,
        )

    with TaskGroup(group_id="gold_layer") as gold:
        gold_aggregation = BashOperator(
            task_id="gold_aggregation",
            bash_command=build_spark_cmd("gold_aggregation.py"),
            retries=2,
        )

    bronze >> silver >> gold
