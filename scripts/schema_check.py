"""
Schema backward-compatibility validation.
Compares schema.sql against a known-good reference (config/schema-reference.json).
Exits 1 on breaking changes: column removal, type change, nullability tightening.
"""
import sys
import os
import json
import re
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BREAKING_CHANGE_EXIT = 1


def parse_sql_schema(sql_path: str) -> dict:
    """Parse CREATE TABLE statements from a SQL file into a structured dict."""
    if not os.path.exists(sql_path):
        logger.error(f"SQL file not found: {sql_path}")
        sys.exit(BREAKING_CHANGE_EXIT)

    with open(sql_path) as f:
        content = f.read()

    tables = {}
    pattern = re.compile(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+\.\w+)\s*\((.*?)\);",
        re.IGNORECASE | re.DOTALL,
    )

    for match in pattern.finditer(content):
        full_name = match.group(1).strip()
        schema, table = full_name.split(".")
        body = match.group(2)

        columns = {}
        for line in body.splitlines():
            line = line.strip()
            if not line or line.upper().startswith(
                ("PRIMARY", "FOREIGN", "INDEX", "UNIQUE",
                 "CONSTRAINT", "CHECK", "--")
            ):
                continue
            col_match = re.match(
                r"(\w+)\s+(\w+(?:\s*\([^)]+\))?)\s*(NOT\s+NULL)?",
                line,
                re.IGNORECASE,
            )
            if col_match:
                col_name = col_match.group(1)
                col_type = col_match.group(2).upper()
                nullable = col_match.group(3) is None
                columns[col_name] = {"type": col_type, "nullable": nullable}

        if columns:
            tables[full_name] = {"columns": columns}

    return tables


def check_compatibility(current: dict, reference: dict) -> bool:
    """Compare current schema against reference. Returns False on breaking changes."""
    ok = True

    for table_name, ref_table in reference.items():
        if table_name not in current:
            logger.warning(f"NEW TABLE (non-breaking): {table_name}")
            continue

        cur_table = current[table_name]
        ref_cols = ref_table.get("columns", {})
        cur_cols = cur_table.get("columns", {})

        for col_name, ref_col in ref_cols.items():
            if col_name not in cur_cols:
                logger.error(f"BREAKING: Column '{col_name}' removed from {table_name}")
                ok = False
                continue

            cur_col = cur_cols[col_name]
            if ref_col["type"].split("(")[0] != cur_col["type"].split("(")[0]:
                logger.error(
                    f"BREAKING: Column '{col_name}' in {table_name} type changed: "
                    f"{ref_col['type']} -> {cur_col['type']}"
                )
                ok = False

            if ref_col["nullable"] is False and cur_col["nullable"] is True:
                logger.error(
                    f"BREAKING: Column '{col_name}' in {table_name} changed "
                    f"from NOT NULL to nullable"
                )
                ok = False

        for col_name in cur_cols:
            if col_name not in ref_cols:
                logger.info(f"NEW COLUMN (non-breaking): {table_name}.{col_name}")

    return ok


def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sql_path = os.path.join(project_root, "sql", "stack_a", "schema.sql")
    ref_path = os.path.join(project_root, "config", "schema-reference.json")

    if not os.path.exists(ref_path):
        logger.error(f"Schema reference not found: {ref_path}")
        sys.exit(BREAKING_CHANGE_EXIT)

    with open(ref_path) as f:
        reference = json.load(f)

    current = {"stack_a": parse_sql_schema(sql_path)}

    logger.info(f"Parsed {len(current['stack_a'])} tables from schema.sql")
    ref_tables = reference.get("stack_a", {})
    logger.info(f"Reference has {len(ref_tables)} tables")

    ok = check_compatibility(current["stack_a"], ref_tables)

    if ok:
        logger.info("Schema compatibility check PASSED")
        sys.exit(0)
    else:
        logger.error("Schema compatibility check FAILED — breaking changes detected")
        sys.exit(BREAKING_CHANGE_EXIT)


if __name__ == "__main__":
    main()
