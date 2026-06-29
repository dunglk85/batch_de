"""Run Great Expectations data quality validation checkpoint."""
import sys
from great_expectations.data_context import DataContext

# Use the Great Expectations config directory in the container
context = DataContext("/home/airflow/config/great_expectations")
results = context.run_checkpoint('transactions_checkpoint')
print(f'Great Expectations validation: {results}')

if not results["success"]:
    sys.exit(1)