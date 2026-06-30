def test_dag_loads():
    from airflow.models import DagBag

    dagbag = DagBag(dag_folder="dags", include_examples=False)
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"


def test_stack_a_dag_structure():
    from airflow.models import DagBag

    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag("ecommerce_dwh_stack_a_pipeline")
    assert dag is not None, "DAG 'ecommerce_dwh_stack_a_pipeline' not found"
    tasks = dag.tasks
    assert len(tasks) > 0, "DAG has no tasks"


def test_stack_a_dag_dependencies():
    from airflow.models import DagBag

    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag("ecommerce_dwh_stack_a_pipeline")
    assert dag is not None
    task_ids = {t.task_id for t in dag.tasks}
    required_tasks = [
        "load_customers",
        "load_products",
        "load_transactions",
        "transform_customers",
        "transform_products",
        "transform_transactions",
        "aggregate_to_gold",
        "reconciliation_check",
        "run_data_quality_checks",
        "publish_prometheus_metrics",
    ]
    for t in required_tasks:
        assert t in task_ids, f"Task '{t}' not found in DAG"
