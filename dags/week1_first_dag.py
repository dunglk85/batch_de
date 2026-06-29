from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'owner': 'dataops-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
}

def dummy_process(**kwargs):
    logger.info("Executing dummy process task")
    execution_date = kwargs['execution_date']
    context = kwargs['ti']
    logger.info(f"Execution date: {execution_date}")
    logger.info(f"Task instance: {context}")
    return {'status': 'success', 'message': 'Dummy process completed'}

dag = DAG(
    'week1_first_dag',
    default_args=DEFAULT_ARGS,
    description='Week 1: First Airflow DAG with dummy tasks',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['week1', 'tutorial'],
)

with dag:
    start = BashOperator(
        task_id='start_pipeline',
        bash_command='echo "Starting Week 1 DAG at $(date)"',
    )

    process = PythonOperator(
        task_id='dummy_process',
        python_callable=dummy_process,
    )

    end = BashOperator(
        task_id='end_pipeline',
        bash_command='echo "Pipeline completed at $(date)"',
    )

    start >> process >> end
