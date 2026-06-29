"""Run Great Expectations data quality validation checkpoint."""
import sys
import os
import json

# Debug: List files in GE config directory
ge_config_dir = "/home/airflow/config/great_expectations"
print(f"GE Config Dir: {ge_config_dir}")
print(f"Exists: {os.path.exists(ge_config_dir)}")

if os.path.exists(ge_config_dir):
    for root, dirs, files in os.walk(ge_config_dir):
        level = root.replace(ge_config_dir, '').count(os.sep)
        indent = ' ' * 2 * level
        print(f"{indent}{os.path.basename(root)}/")
        subindent = ' ' * 2 * (level + 1)
        for file in files:
            print(f"{subindent}{file}")

# Check expectation suite files
for suite in ['silver_transactions_suite.json', 'silver_customers_suite.json', 'silver_products_suite.json']:
    path = os.path.join(ge_config_dir, 'expectations', suite)
    if os.path.exists(path):
        with open(path) as f:
            content = f.read()
            print(f"\n{suite} (length: {len(content)}):")
            print(content[:200])
    else:
        print(f"MISSING: {path}")

from great_expectations.data_context import DataContext

context = DataContext(ge_config_dir)

# List available checkpoints
checkpoints = context.list_checkpoints()
print(f"\nAvailable checkpoints: {checkpoints}")

# List expectation suites
suites = context.list_expectation_suites()
print(f"Available expectation suites: {suites}")

results = context.run_checkpoint('transactions_checkpoint')
print(f'\nGreat Expectations validation: {results}')

if not results["success"]:
    sys.exit(1)