"""
Great Expectations Data Quality Validation Checkpoint Runner.
Blocks pipeline on critical expectation failures with detailed reporting.
"""
import sys
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CHECKPOINT_NAME = "transactions_checkpoint"
CRITICAL_EXPECTATIONS = {
    "expect_column_values_to_not_be_null",
    "expect_column_values_to_be_unique",
    "expect_column_values_to_be_in_set",
}
WARNING_EXPECTATIONS = {
    "expect_table_row_count_to_be_between",
    "expect_column_mean_to_be_between",
    "expect_column_values_to_be_between",
}


def validate_checkpoint_result(results: dict) -> int:
    run_results = results.get("run_results", {})

    if not run_results:
        logger.error("No run_results in checkpoint response")
        return 1

    exit_code = 0
    total_failures = 0
    critical_failures = 0

    for batch_id, batch_result in run_results.items():
        validation_result = batch_result.get("validation_result", {})
        suite_name = validation_result.get("meta", {}).get("expectation_suite_name", "unknown")
        statistics = validation_result.get("statistics", {})
        evaluated = statistics.get("evaluated_expectations", 0)
        failed = statistics.get("failed_expectations", 0)

        logger.info(f"Suite: {suite_name} | Evaluated: {evaluated} | Failed: {failed}")

        if failed == 0:
            continue

        total_failures += failed
        results_list = validation_result.get("results", [])
        for res in results_list:
            if res.get("success", True):
                continue
            exp_type = res.get("expectation_config", {}).get("expectation_type", "unknown")
            exp_kwargs = res.get("expectation_config", {}).get("kwargs", {})
            col = exp_kwargs.get("column", "N/A")
            msg = f"  FAILED | {exp_type} | column={col}"

            exception_info = res.get("exception_info", {})
            if exception_info.get("raised_exception"):
                msg += f" | exception={exception_info.get('exception_message', '')}"
            msg += f" | value={res.get('result', {})}"

            if exp_type in CRITICAL_EXPECTATIONS:
                critical_failures += 1
                logger.error(f"[CRITICAL] {msg}")
            elif exp_type in WARNING_EXPECTATIONS:
                logger.warning(f"[WARNING] {msg}")
            else:
                logger.warning(f"[INFO] {msg}")

    if critical_failures > 0:
        logger.error(
            f"BLOCKING PIPELINE: {critical_failures} critical "
            f"expectation(s) failed (total failures: {total_failures})"
        )
        exit_code = 1
    elif total_failures > 0:
        logger.warning(
            f"Validation warnings only (no critical failures): "
            f"{total_failures} non-critical failure(s)"
        )
    else:
        logger.info("All expectations passed successfully")

    return exit_code


def main():
    ge_config_dir = "/home/airflow/config/great_expectations"

    if not os.path.exists(ge_config_dir):
        logger.error(f"GE config directory not found: {ge_config_dir}")
        sys.exit(1)

    # Validate that required expectation suites exist
    required_suites = [
        "silver_transactions_suite.json",
        "silver_customers_suite.json",
        "silver_products_suite.json",
    ]
    missing = []
    for suite in required_suites:
        path = os.path.join(ge_config_dir, "expectations", suite)
        if not os.path.exists(path):
            missing.append(suite)
    if missing:
        logger.error(f"Missing expectation suites: {missing}")
        sys.exit(1)

    from great_expectations.data_context import DataContext

    context = DataContext(ge_config_dir)
    checkpoints = context.list_checkpoints()
    logger.info(f"Available checkpoints: {checkpoints}")

    if CHECKPOINT_NAME not in checkpoints:
        logger.error(f"Checkpoint '{CHECKPOINT_NAME}' not found in {checkpoints}")
        sys.exit(1)

    results = context.run_checkpoint(CHECKPOINT_NAME)
    logger.info(f"Checkpoint '{CHECKPOINT_NAME}' execution complete")

    exit_code = validate_checkpoint_result(results)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
