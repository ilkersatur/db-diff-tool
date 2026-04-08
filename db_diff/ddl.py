from __future__ import annotations

from typing import Any

import pandas as pd

from db_diff.services import quote_ident, qualified_table_name


def generate_schema_sync_sql(
    table_name: str, dev_cols: dict[str, str], test_cols: dict[str, str]
) -> list[str]:
    sql: list[str] = []
    qualified = qualified_table_name(table_name)

    if not test_cols and dev_cols:
        col_defs = ",\n    ".join(
            f"{quote_ident(col)} {dtype}" for col, dtype in sorted(dev_cols.items())
        )
        sql.append(f"CREATE TABLE {qualified} (\n    {col_defs}\n);")
        return sql

    for col, dtype in sorted(dev_cols.items()):
        if col not in test_cols:
            sql.append(f"ALTER TABLE {qualified} ADD {quote_ident(col)} {dtype};")
        elif test_cols[col] != dtype:
            sql.append(f"ALTER TABLE {qualified} ALTER COLUMN {quote_ident(col)} {dtype};")
    return sql


def _sql_literal(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _where_clause_from_key(row: pd.Series, key_cols: list[str]) -> str:
    return " AND ".join(f"{quote_ident(k)}={_sql_literal(row[k])}" for k in key_cols)


def generate_data_sync_sql(
    table_name: str,
    diff_result: dict[str, Any],
    key_cols: list[str],
    common_cols: list[str],
) -> list[str]:
    sql: list[str] = []
    qualified = qualified_table_name(table_name)

    for _, row in diff_result["only_dev"].iterrows():
        cols = ", ".join(quote_ident(c) for c in common_cols)
        values = ", ".join(_sql_literal(row[c]) for c in common_cols)
        sql.append(f"INSERT INTO {qualified} ({cols}) VALUES ({values});")

    for item in diff_result["changed"]:
        key_value = item["key"]
        if not isinstance(key_value, tuple):
            key_value = (key_value,)
        key_map = dict(zip(key_cols, key_value))
        set_parts = []
        for col, values in item["diffs"].items():
            set_parts.append(f"{quote_ident(col)}={_sql_literal(values['dev'])}")
        where_parts = [f"{quote_ident(k)}={_sql_literal(v)}" for k, v in key_map.items()]
        sql.append(
            f"UPDATE {qualified} SET {', '.join(set_parts)} WHERE {' AND '.join(where_parts)};"
        )

    for _, row in diff_result["only_test"].iterrows():
        sql.append(f"DELETE FROM {qualified} WHERE {_where_clause_from_key(row, key_cols)};")

    return sql


def build_preview_summary(
    col_diff_df: pd.DataFrame, diff_result: dict[str, Any] | None
) -> dict[str, int]:
    summary = {
        "missing_in_test_columns": 0,
        "missing_in_dev_columns": 0,
        "different_type_columns": 0,
        "rows_insert": 0,
        "rows_update": 0,
        "rows_delete": 0,
    }

    if not col_diff_df.empty:
        summary["missing_in_test_columns"] = int(
            (col_diff_df["status"] == "missing_in_test").sum()
        )
        summary["missing_in_dev_columns"] = int(
            (col_diff_df["status"] == "missing_in_dev").sum()
        )
        summary["different_type_columns"] = int((col_diff_df["status"] == "different").sum())

    if diff_result:
        summary["rows_insert"] = int(len(diff_result["only_dev"]))
        summary["rows_update"] = int(len(diff_result["changed"]))
        summary["rows_delete"] = int(len(diff_result["only_test"]))

    return summary

