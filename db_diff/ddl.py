from __future__ import annotations

from typing import Any

import pandas as pd

from db_diff.services import quote_ident, qualified_table_name


def _normalize_type_sql(dtype: str) -> str:
    # SQLAlchemy may return collation names wrapped in double quotes on MSSQL.
    return (dtype or "").replace('"', "")


def generate_schema_sync_sql(
    table_name: str,
    dev_cols: dict[str, str],
    test_cols: dict[str, str],
    dev_table_def: dict[str, Any] | None = None,
) -> list[str]:
    sql: list[str] = []
    qualified = qualified_table_name(table_name)

    if not test_cols and dev_cols:
        if dev_table_def:
            sql.extend(generate_create_table_sql(dev_table_def))
        else:
            col_defs = ",\n    ".join(
                f"{quote_ident(col)} {dtype}" for col, dtype in sorted(dev_cols.items())
            )
            sql.append(f"CREATE TABLE {qualified} (\n    {col_defs}\n);")
        return sql

    for col, dtype in sorted(dev_cols.items()):
        if col not in test_cols:
            sql.append(f"ALTER TABLE {qualified} ADD {quote_ident(col)} {_normalize_type_sql(dtype)};")
        elif test_cols[col] != dtype:
            sql.append(
                f"ALTER TABLE {qualified} ALTER COLUMN {quote_ident(col)} {_normalize_type_sql(dtype)};"
            )

    # DEV is treated as source of truth: columns that exist only in TEST are removed.
    for col in sorted(test_cols.keys()):
        if col not in dev_cols:
            sql.append(f"ALTER TABLE {qualified} DROP COLUMN {quote_ident(col)};")
    return sql


def generate_create_table_sql(table_def: dict[str, Any]) -> list[str]:
    schema_name = table_def["schema"]
    table_name = table_def["table"]
    qualified = f"{quote_ident(schema_name)}.{quote_ident(table_name)}"
    lines: list[str] = []
    lines.append(f"-- {schema_name}.{table_name} definition")
    lines.append("")
    lines.append("-- Drop table")
    lines.append(f"-- DROP TABLE {qualified};")
    lines.append("")

    col_parts = []
    for col in table_def["columns"]:
        nullable = "NULL" if col["nullable"] else "NOT NULL"
        col_parts.append(
            f"\t{quote_ident(col['name'])} {_normalize_type_sql(col['type'])} {nullable}"
        )

    pk_cols = table_def.get("pk_columns") or []
    if pk_cols:
        pk_name = table_def.get("pk_name") or f"PK_{table_name}"
        joined_pk_cols = ", ".join(quote_ident(c) for c in pk_cols)
        col_parts.append(f"\tCONSTRAINT {quote_ident(pk_name)} PRIMARY KEY ({joined_pk_cols})")

    lines.append(f"CREATE TABLE {qualified} (")
    lines.append(",\n".join(col_parts))
    lines.append(");")

    for idx in table_def.get("indexes", []):
        idx_name = idx.get("name")
        idx_cols = idx.get("column_names") or []
        if not idx_name or not idx_cols:
            continue
        idx_kind = "UNIQUE NONCLUSTERED INDEX" if idx.get("unique") else "NONCLUSTERED INDEX"
        joined_cols = ", ".join(quote_ident(c) for c in idx_cols)
        lines.append(f"CREATE {idx_kind} {quote_ident(idx_name)} ON {qualified} ({joined_cols});")

    return lines


def generate_sequence_sync_sql(
    dev_sequences: dict[str, dict[str, Any]], test_sequences: dict[str, dict[str, Any]]
) -> list[str]:
    sql: list[str] = []
    dev_keys = set(dev_sequences.keys())
    test_keys = set(test_sequences.keys())

    for seq_name in sorted(dev_keys - test_keys):
        seq = dev_sequences[seq_name]
        qualified = f"{seq['schema_name']}.{seq['sequence_name']}"
        cycle_sql = "CYCLE" if seq.get("is_cycling") else "NO CYCLE"
        sql.append(f"CREATE SEQUENCE {qualified}")
        sql.append(f"   START WITH {seq.get('start_value', 1)}")
        sql.append(f"   INCREMENT BY {seq.get('increment_value', 1)}")
        sql.append(f"   {cycle_sql};")
        sql.append("")

    # DEV source of truth: extra sequences in TEST are dropped.
    for seq_name in sorted(test_keys - dev_keys):
        schema_name, simple_name = seq_name.split(".", 1)
        qualified = f"{quote_ident(schema_name)}.{quote_ident(simple_name)}"
        sql.append(f"DROP SEQUENCE {qualified};")

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

