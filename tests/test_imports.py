def test_import_dags():
    import importlib

    spec = importlib.util.find_spec("dags.stack_a_dwh_pipeline")
    assert spec is not None, "DAG module not found"


def test_data_directories_exist():
    import os

    for d in ["data/raw", "data/bronze", "data/silver", "data/gold"]:
        assert os.path.isdir(d), f"Missing directory: {d}"
