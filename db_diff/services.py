from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

SYSTEM_SCHEMAS = {"INFORMATION_SCHEMA", "sys"}


@dataclass(frozen=True)
class TableColumnDiff:
    column: str
    dev_type: str
    test_type: str
    status: str


def normalize_connection_string(conn_str: str) -> str:
    raw = (conn_str or "").strip()
    if not raw:
        return raw

    if "://" in raw:
        return raw

    parts: dict[str, str] = {}
    for item in raw.split(";"):
        item = item.strip()
        if not item or "=" not in item:
            continue
        key, value = item.split("=", 1)
        parts[key.strip().lower()] = value.strip()

    server = parts.get("data source") or parts.get("server")
    database = parts.get("initial catalog") or parts.get("database")
    if not server or not database:
        return raw

    integrated_security = (parts.get("integrated security") or "").lower() in {
        "true",
        "yes",
        "sspi",
    }

    params = [f"DRIVER={{{parts.get('driver', 'ODBC Driver 17 for SQL Server')}}}"]
    if integrated_security:
        params.append("Trusted_Connection=yes")
    else:
        user = parts.get("user id") or parts.get("uid")
        password = parts.get("password") or parts.get("pwd")
        if user and password:
            params.extend([f"UID={user}", f"PWD={password}"])

    if (parts.get("trustservercertificate") or "").lower() in {"true", "yes"}:
        params.append("TrustServerCertificate=yes")

    odbc_str = f"SERVER={server};DATABASE={database};" + ";".join(params) + ";"
    return f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"


def make_engine(conn_str: str) -> Engine:
    return create_engine(normalize_connection_string(conn_str), pool_pre_ping=True)


def read_schema(engine: Engine) -> tuple[list[str], dict[str, dict[str, str]]]:
    inspector = inspect(engine)
    tables: list[str] = []
    schema_map: dict[str, dict[str, str]] = {}

    for schema_name in inspector.get_schema_names():
        if schema_name in SYSTEM_SCHEMAS:
            continue
        for table in inspector.get_table_names(schema=schema_name):
            full_name = f"{schema_name}.{table}"
            tables.append(full_name)
            cols = inspector.get_columns(table, schema=schema_name)
            schema_map[full_name] = {
                col["name"]: str(col.get("type", "UNKNOWN")).upper() for col in cols
            }

    return sorted(tables), schema_map


def compare_table_columns(dev_cols: dict[str, str], test_cols: dict[str, str]) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for col_name in sorted(set(dev_cols) | set(test_cols)):
        dev_type = dev_cols.get(col_name)
        test_type = test_cols.get(col_name)
        if dev_type and not test_type:
            status = "missing_in_test"
        elif test_type and not dev_type:
            status = "missing_in_dev"
        elif dev_type == test_type:
            status = "matching"
        else:
            status = "different"

        rows.append(
            {
                "column": col_name,
                "dev_type": dev_type or "-",
                "test_type": test_type or "-",
                "status": status,
            }
        )
    return pd.DataFrame(rows)


def quote_ident(name: str) -> str:
    return f"[{name.replace(']', ']]')}]"


def qualified_table_name(full_table_name: str) -> str:
    if "." not in full_table_name:
        return quote_ident(full_table_name)
    schema_name, table_name = full_table_name.split(".", 1)
    return f"{quote_ident(schema_name)}.{quote_ident(table_name)}"


def read_table_data(engine: Engine, table_name: str, limit: int) -> pd.DataFrame:
    query = text(f"SELECT * FROM {qualified_table_name(table_name)}")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df.head(limit) if limit and limit > 0 else df


def _normalize_scalar(value: Any) -> Any:
    if pd.isna(value):
        return None
    return str(value)


def compare_dataframes(
    dev_df: pd.DataFrame, test_df: pd.DataFrame, key_cols: list[str]
) -> dict[str, Any]:
    dev_pref = dev_df.copy().set_index(key_cols, drop=False)
    test_pref = test_df.copy().set_index(key_cols, drop=False)

    only_dev_idx = dev_pref.index.difference(test_pref.index)
    only_test_idx = test_pref.index.difference(dev_pref.index)
    common_idx = dev_pref.index.intersection(test_pref.index)

    common_cols = [c for c in dev_df.columns if c in test_df.columns]
    changed_rows: list[dict[str, Any]] = []

    for idx in common_idx:
        dev_row = dev_pref.loc[idx, common_cols]
        test_row = test_pref.loc[idx, common_cols]

        if isinstance(dev_row, pd.DataFrame) or isinstance(test_row, pd.DataFrame):
            continue

        diffs = {}
        for col in common_cols:
            dev_val = _normalize_scalar(dev_row[col])
            test_val = _normalize_scalar(test_row[col])
            if dev_val != test_val:
                diffs[col] = {"dev": dev_val, "test": test_val}
        if diffs:
            changed_rows.append({"key": idx, "diffs": diffs})

    return {
        "only_dev": dev_pref.loc[only_dev_idx].reset_index(drop=True),
        "only_test": test_pref.loc[only_test_idx].reset_index(drop=True),
        "changed": changed_rows,
        "common_columns": common_cols,
    }

