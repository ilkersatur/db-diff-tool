from __future__ import annotations

from typing import Any
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

SYSTEM_SCHEMAS = {"INFORMATION_SCHEMA", "sys"}


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
    bundle = read_schema_bundle(engine)
    return bundle["tables"], bundle["schema_map"]


def read_schema_bundle(engine: Engine) -> dict[str, Any]:
    inspector = inspect(engine)
    tables: list[str] = []
    schema_map: dict[str, dict[str, str]] = {}
    table_defs: dict[str, dict[str, Any]] = {}
    sequence_map: dict[str, dict[str, Any]] = {}

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
            pk = inspector.get_pk_constraint(table, schema=schema_name) or {}
            indexes = inspector.get_indexes(table, schema=schema_name) or []
            table_defs[full_name] = {
                "schema": schema_name,
                "table": table,
                "columns": [
                    {
                        "name": col["name"],
                        "type": str(col.get("type", "UNKNOWN")).upper(),
                        "nullable": bool(col.get("nullable", True)),
                    }
                    for col in cols
                ],
                "pk_name": pk.get("name"),
                "pk_columns": pk.get("constrained_columns") or [],
                "indexes": indexes,
            }

    # SQL Server sequence metadata (if accessible on this engine/user).
    try:
        seq_query = text(
            """
            SELECT
                s.name AS schema_name,
                seq.name AS sequence_name,
                UPPER(t.name) AS data_type,
                seq.start_value,
                seq.[increment] AS increment_value,
                seq.minimum_value,
                seq.maximum_value,
                seq.is_cycling
            FROM sys.sequences seq
            INNER JOIN sys.schemas s ON s.schema_id = seq.schema_id
            INNER JOIN sys.types t ON t.user_type_id = seq.user_type_id
            """
        )
        with engine.connect() as conn:
            for row in conn.execute(seq_query).mappings():
                schema_name = row["schema_name"]
                if schema_name in SYSTEM_SCHEMAS:
                    continue
                full_name = f"{schema_name}.{row['sequence_name']}"
                sequence_map[full_name] = dict(row)
    except Exception:
        sequence_map = {}

    # Fallback: if metadata query fails or returns empty, at least collect sequence names.
    if not sequence_map:
        try:
            for schema_name in inspector.get_schema_names():
                if schema_name in SYSTEM_SCHEMAS:
                    continue
                seq_names = inspector.get_sequence_names(schema=schema_name) or []
                for seq_name in seq_names:
                    full_name = f"{schema_name}.{seq_name}"
                    sequence_map[full_name] = {
                        "schema_name": schema_name,
                        "sequence_name": seq_name,
                        "data_type": "BIGINT",
                        "start_value": 1,
                        "increment_value": 1,
                        "minimum_value": 1,
                        "maximum_value": 9223372036854775807,
                        "is_cycling": False,
                    }
        except Exception:
            sequence_map = {}

    return {
        "tables": sorted(tables),
        "schema_map": schema_map,
        "table_defs": table_defs,
        "sequences": sequence_map,
    }


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

