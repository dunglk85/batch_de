"""Run Great Expectations data quality validation checkpoint."""
import sys
import os

# Set GE config path explicitly
os.environ["GE_CONFIG_DIR"] = "/home/airflow/config/great_expectations"

from great_expectations.data_context import DataContext

context = DataContext("/home/airflow/config/great_expectations")

# List available checkpoints for debugging
checkpoints = context.list_checkpoints()
print(f"Available checkpoints: {checkpoints}")

results = context.run_checkpoint('transactions_checkpoint')
print(f'Great Expectations validation: {results}')

if not results["success"]:
    sys.exit(1)