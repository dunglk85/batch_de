"""
Stack B: Lakehouse Pipeline (PySpark + Delta Lake)
Medallion Architecture: Bronze -> Silver -> Gold
Orchestrated with Apache Airflow via PythonOperator + PySpark
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import logging
import sys
import os

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

PYSPARK_DIR = "/home/airflow/pyspark/stack_b"
sys.path.insert(0, PYSPARK_DIR)


def run_bronze(**kwargs):
    from bronze_ingestion import main
    main()


def run_silver(**kwargs):
    from silver_transformation import main
    main()


def run_gold(**kwargs):
    from gold_aggregation import main
    main()


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
        bronze_ingestion = PythonOperator(
            task_id="bronze_ingestion",
            python_callable=run_bronze,
            retries=2,
        )

    with TaskGroup(group_id="silver_layer") as silver:
        silver_transformation = PythonOperator(
            task_id="silver_transformation",
            python_callable=run_silver,
            retries=2,
        )

    with TaskGroup(group_id="gold_layer") as gold:
        gold_aggregation = PythonOperator(
            task_id="gold_aggregation",
            python_callable=run_gold,
            retries=2,
        )

    bronze >> silver >> gold
