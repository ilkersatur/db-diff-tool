from __future__ import annotations

import time

import pandas as pd
import streamlit as st
from sqlalchemy.exc import SQLAlchemyError

from db_diff.ddl import (
    build_preview_summary,
    generate_data_sync_sql,
    generate_schema_sync_sql,
    generate_sequence_sync_sql,
)
from db_diff.services import (
    compare_dataframes,
    compare_table_columns,
    make_engine,
    read_schema_bundle,
    read_table_data,
)
from db_diff.state import init_state
from db_diff.ui import STATUS_LABELS, inject_style, render_status_legend, style_diff_table

st.set_page_config(page_title="Database Diff Studio", page_icon=":material/compare:", layout="wide")
init_state()
inject_style()
if "is_connecting" not in st.session_state:
    st.session_state.is_connecting = False
if "last_connected_dev_conn" not in st.session_state:
    st.session_state.last_connected_dev_conn = ""
if "last_connected_test_conn" not in st.session_state:
    st.session_state.last_connected_test_conn = ""


@st.cache_resource(show_spinner=False)
def get_engine(conn_str: str):
    return make_engine(conn_str)


@st.cache_data(show_spinner=False, ttl=300)
def get_schema_snapshot(conn_str: str):
    return read_schema_bundle(get_engine(conn_str))


def get_table_snapshot(conn_str: str, table_name: str, limit: int):
    # Do not cache row-level table data.
    return read_table_data(get_engine(conn_str), table_name, limit)


def table_with_presence(dev_tables: list[str], test_tables: list[str]) -> pd.DataFrame:
    rows = []
    for table in sorted(set(dev_tables) | set(test_tables)):
        in_dev = table in dev_tables
        in_test = table in test_tables
        if in_dev and in_test:
            status = "matching"
        elif in_dev:
            status = "missing_in_test"
        else:
            status = "missing_in_dev"
        rows.append({"table": table, "status": status})
    return pd.DataFrame(rows)


def build_sql_blob(sql_lines: list[str]) -> str:
    if not sql_lines:
        return "-- No actions generated."
    return "\n".join(sql_lines)


def format_seconds(value: float) -> str:
    secs = max(0, int(round(value)))
    minutes, seconds = divmod(secs, 60)
    return f"{minutes:02d}:{seconds:02d}"


def suggest_key_columns(
    dev_table_def: dict | None, test_table_def: dict | None, common_cols: list[str]
) -> list[str]:
    common_set = set(common_cols)
    candidates: list[list[str]] = []

    if dev_table_def:
        dev_pk = [c for c in (dev_table_def.get("pk_columns") or []) if c in common_set]
        if dev_pk:
            candidates.append(dev_pk)
        for idx in dev_table_def.get("indexes", []):
            if idx.get("unique"):
                cols = [c for c in (idx.get("column_names") or []) if c in common_set]
                if cols:
                    candidates.append(cols)

    if test_table_def:
        test_pk = [c for c in (test_table_def.get("pk_columns") or []) if c in common_set]
        if test_pk:
            candidates.append(test_pk)
        for idx in test_table_def.get("indexes", []):
            if idx.get("unique"):
                cols = [c for c in (idx.get("column_names") or []) if c in common_set]
                if cols:
                    candidates.append(cols)

    return candidates[0] if candidates else []


def render_quick_table_selector(
    title: str, tables: list[str], table_key: str, expanded: bool = False
) -> None:
    with st.expander(title, expanded=expanded):
        if tables:
            event = st.dataframe(
                pd.DataFrame({"table": tables}),
                use_container_width=True,
                hide_index=True,
                on_select="rerun",
                selection_mode="single-row",
                key=table_key,
            )
            selected_rows = event.selection.get("rows", []) if event and event.selection else []
            if selected_rows:
                picked_table = tables[selected_rows[0]]
                if st.session_state.selected_table != picked_table:
                    st.session_state.selected_table = picked_table
                    st.rerun()
            st.caption("Tip: Click a row to set active table.")
        else:
            st.caption("No tables in this list.")


st.title("Database Diff Studio")
st.caption("Compare DEV and TEST schemas/data and generate sync-ready SQL scripts.")

with st.sidebar:
    st.subheader("Workflow")
    st.caption("1) Connect -> 2) Compare -> 3) Generate SQL")
    st.checkbox("Show only differences", key="show_only_differences")
    if st.button("Clear cache", use_container_width=True):
        get_schema_snapshot.clear()
        get_engine.clear()
        st.session_state.connected = False
        st.session_state.dev_tables = []
        st.session_state.test_tables = []
        st.session_state.dev_schema = {}
        st.session_state.test_schema = {}
        st.session_state.dev_table_defs = {}
        st.session_state.test_table_defs = {}
        st.session_state.dev_sequences = {}
        st.session_state.test_sequences = {}
        st.session_state.data_diff_result = None
        st.session_state.generated_sql = ""
        st.session_state.generated_sequence_sql = ""
        st.session_state.last_connected_dev_conn = ""
        st.session_state.last_connected_test_conn = ""
        st.success("Cache cleared.")
        st.rerun()
    st.divider()
    st.subheader("About this tool")
    st.markdown(
        """
        - Connects to DEV and TEST databases
        - Compares tables between environments
        - Shows missing tables on each side
        - Compares column existence and data types
        - Highlights column/type differences by table
        - Compares row-level data with selected keys
        - Generates schema sync SQL (CREATE/ALTER/DROP)
        - Generates data sync SQL (INSERT/UPDATE/DELETE)
        - Compares sequences and generates sequence SQL
        - Uses DEV as source of truth for sync scripts
        """
    )

if "generated_sequence_sql" not in st.session_state:
    st.session_state.generated_sequence_sql = ""

tab_connect, tab_compare, tab_sql, tab_sequence = st.tabs(
    ["Connect", "Compare", "DDL & Sync Script", "Sequences"]
)

with tab_connect:
    st.markdown("### Step 1 - Connect to environments")
    left, right = st.columns(2)
    with left:
        st.markdown("<div class='panel'><div class='dev-title'>DEV Connection</div></div>", unsafe_allow_html=True)
        dev_conn = st.text_input(
            "DEV connection string",
            type="password",
            key="dev_conn",
            help="SQLAlchemy URL or ADO.NET style string.",
        )
    with right:
        st.markdown("<div class='panel'><div class='test-title'>TEST Connection</div></div>", unsafe_allow_html=True)
        test_conn = st.text_input(
            "TEST connection string",
            type="password",
            key="test_conn",
            help="SQLAlchemy URL or ADO.NET style string.",
        )

    connect_clicked = st.button(
        "Connect and load schemas",
        type="primary",
        disabled=st.session_state.is_connecting,
    )
    if connect_clicked:
        if not dev_conn or not test_conn:
            st.error("Both DEV and TEST connection strings are required.")
        elif (
            st.session_state.connected
            and st.session_state.last_connected_dev_conn == dev_conn
            and st.session_state.last_connected_test_conn == test_conn
        ):
            st.info("Already connected with same connection strings. Using cached metadata.")
        else:
            st.session_state.is_connecting = True
            bar = st.progress(0, text="Starting connection checks...")
            status_box = st.empty()
            try:
                started_at = time.perf_counter()
                bar.progress(20, text="Connecting DEV")
                status_box.info("Step 1/2: Connecting DEV...")
                dev_started = time.perf_counter()
                dev_bundle = get_schema_snapshot(dev_conn)
                dev_elapsed = time.perf_counter() - dev_started
                total_elapsed = time.perf_counter() - started_at

                bar.progress(60, text="Connecting TEST")
                status_box.info(
                    "Step 2/2: Connecting TEST...\n\n"
                    f"Elapsed: {format_seconds(total_elapsed)} | "
                    f"Estimated remaining: {format_seconds(dev_elapsed)}"
                )
                test_bundle = get_schema_snapshot(test_conn)
                final_elapsed = time.perf_counter() - started_at
                bar.progress(100, text="Connection complete")
                status_box.success(f"Connected successfully in {format_seconds(final_elapsed)}.")

                st.session_state.connected = True
                st.session_state.dev_tables = dev_bundle["tables"]
                st.session_state.test_tables = test_bundle["tables"]
                st.session_state.dev_schema = dev_bundle["schema_map"]
                st.session_state.test_schema = test_bundle["schema_map"]
                st.session_state.dev_table_defs = dev_bundle["table_defs"]
                st.session_state.test_table_defs = test_bundle["table_defs"]
                st.session_state.dev_sequences = dev_bundle["sequences"]
                st.session_state.test_sequences = test_bundle["sequences"]
                st.session_state.last_connected_dev_conn = dev_conn
                st.session_state.last_connected_test_conn = test_conn
                st.success("Connections successful. Move to Compare step.")
            except SQLAlchemyError as exc:
                st.session_state.connected = False
                status_box.empty()
                st.error(f"Database error while connecting: {exc}")
            except Exception as exc:  # pragma: no cover
                st.session_state.connected = False
                status_box.empty()
                st.error(f"Unexpected connection error: {exc}")
            finally:
                st.session_state.is_connecting = False

with tab_compare:
    st.markdown("### Step 2 - Compare schema and data")
    if not st.session_state.connected:
        st.info("Open Connect step and load both schemas first.")
    else:
        dev_tables = st.session_state.dev_tables
        test_tables = st.session_state.test_tables
        dev_schema = st.session_state.dev_schema
        test_schema = st.session_state.test_schema

        table_df = table_with_presence(dev_tables, test_tables)
        search_term = ""
        if search_term:
            table_df = table_df[table_df["table"].str.lower().str.contains(search_term)]
        if st.session_state.show_only_differences:
            table_df = table_df[table_df["status"] != "matching"]

        m1, m2, m3 = st.columns(3)
        m1.metric("Tables in both", int((table_df["status"] == "matching").sum()))
        m2.metric("Missing in TEST", int((table_df["status"] == "missing_in_test").sum()))
        m3.metric("Missing in DEV", int((table_df["status"] == "missing_in_dev").sum()))
        render_status_legend()

        missing_in_test_tables = table_df.loc[
            table_df["status"] == "missing_in_test", "table"
        ].tolist()
        missing_in_dev_tables = table_df.loc[
            table_df["status"] == "missing_in_dev", "table"
        ].tolist()

        lcol, rcol = st.columns(2)
        with lcol:
            render_quick_table_selector(
                f"Missing in TEST tables ({len(missing_in_test_tables)})",
                missing_in_test_tables,
                "missing_test_table_click",
                expanded=True,
            )
        with rcol:
            render_quick_table_selector(
                f"Missing in DEV tables ({len(missing_in_dev_tables)})",
                missing_in_dev_tables,
                "missing_dev_table_click",
                expanded=True,
            )

        column_diff_tables: list[str] = []
        type_diff_tables: list[str] = []
        for table_name in sorted(set(dev_tables) & set(test_tables)):
            table_diff_df = compare_table_columns(
                dev_schema.get(table_name, {}), test_schema.get(table_name, {})
            )
            if table_diff_df.empty:
                continue
            has_column_presence_diff = table_diff_df["status"].isin(
                ["missing_in_test", "missing_in_dev"]
            ).any()
            has_type_diff = (table_diff_df["status"] == "different").any()
            if has_column_presence_diff:
                column_diff_tables.append(table_name)
            if has_type_diff:
                type_diff_tables.append(table_name)

        dcol, tcol = st.columns(2)
        with dcol:
            render_quick_table_selector(
                f"Tables with missing/different columns ({len(column_diff_tables)})",
                column_diff_tables,
                "column_diff_table_click",
                expanded=False,
            )
        with tcol:
            render_quick_table_selector(
                f"Tables with column type differences ({len(type_diff_tables)})",
                type_diff_tables,
                "type_diff_table_click",
                expanded=False,
            )

        table_options = table_df["table"].tolist()
        if not table_options:
            st.warning("No tables match current filters.")
        else:
            selected_table = st.selectbox(
                "Select table",
                table_options,
                index=table_options.index(st.session_state.selected_table)
                if st.session_state.selected_table in table_options
                else 0,
            )
            st.session_state.selected_table = selected_table

            dev_cols = dev_schema.get(selected_table, {})
            test_cols = test_schema.get(selected_table, {})
            col_diff_df = compare_table_columns(dev_cols, test_cols)

            if search_term:
                col_diff_df = col_diff_df[
                    col_diff_df["column"].str.lower().str.contains(search_term)
                    | col_diff_df["dev_type"].str.lower().str.contains(search_term)
                    | col_diff_df["test_type"].str.lower().str.contains(search_term)
                ]
            if st.session_state.show_only_differences:
                col_diff_df = col_diff_df[col_diff_df["status"] != "matching"]

            st.markdown("#### Column diff")
            st.dataframe(style_diff_table(col_diff_df), use_container_width=True, hide_index=True)

            left, right = st.columns(2)
            with left:
                if not dev_cols:
                    st.warning("Selected table does not exist in DEV.")
            with right:
                if not test_cols:
                    st.warning("Selected table does not exist in TEST.")

            st.markdown("#### Data diff (optional)")
            st.toggle("Enable data comparison", key="data_compare_enabled")
            if st.session_state.data_compare_enabled and dev_cols and test_cols:
                common_cols = sorted(set(dev_cols) & set(test_cols))
                dev_table_def = st.session_state.get("dev_table_defs", {}).get(selected_table)
                test_table_def = st.session_state.get("test_table_defs", {}).get(selected_table)
                auto_keys = suggest_key_columns(dev_table_def, test_table_def, common_cols)

                if "key_selection_table" not in st.session_state:
                    st.session_state.key_selection_table = ""
                if (
                    st.session_state.key_selection_table != selected_table
                    or not st.session_state.selected_keys
                ):
                    st.session_state.selected_keys = auto_keys
                    st.session_state.key_selection_table = selected_table

                if auto_keys:
                    st.caption(
                        f"Auto key suggestion from PK/UNIQUE index: {', '.join(auto_keys)}"
                    )
                else:
                    st.caption("No PK/UNIQUE index detected for automatic key suggestion.")
                st.multiselect(
                    "Key columns (for row matching)",
                    common_cols,
                    key="selected_keys",
                )
                if st.button("Run data comparison", type="secondary"):
                    if not st.session_state.selected_keys:
                        st.warning("Select at least one key column.")
                    else:
                        bar = st.progress(0, text="Reading DEV table data...")
                        try:
                            with st.spinner("Loading table snapshots..."):
                                dev_df = get_table_snapshot(
                                    st.session_state.dev_conn,
                                    selected_table,
                                    0,
                                )
                                bar.progress(50, text="Reading TEST table data...")
                                test_df = get_table_snapshot(
                                    st.session_state.test_conn,
                                    selected_table,
                                    0,
                                )
                            bar.progress(90, text="Comparing rows...")
                            st.session_state.data_diff_result = compare_dataframes(
                                dev_df, test_df, st.session_state.selected_keys
                            )
                            bar.progress(100, text="Comparison done")
                        except SQLAlchemyError as exc:
                            st.error(f"Database error during data compare: {exc}")
                        except Exception as exc:  # pragma: no cover
                            st.error(f"Unexpected error during data compare: {exc}")

                if st.session_state.data_diff_result:
                    result = st.session_state.data_diff_result
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Only in DEV rows", len(result["only_dev"]))
                    c2.metric("Only in TEST rows", len(result["only_test"]))
                    c3.metric("Changed rows", len(result["changed"]))

                    with st.expander("Rows only in DEV", expanded=False):
                        st.dataframe(result["only_dev"], use_container_width=True)
                    with st.expander("Rows only in TEST", expanded=False):
                        st.dataframe(result["only_test"], use_container_width=True)
                    with st.expander("Changed rows", expanded=True):
                        if result["changed"]:
                            changed_rows_flat: list[dict[str, str]] = []
                            key_cols = st.session_state.selected_keys
                            for item in result["changed"]:
                                key_value = item["key"]
                                if not isinstance(key_value, tuple):
                                    key_value = (key_value,)
                                key_map = dict(zip(key_cols, key_value))
                                for col_name, diff_values in item["diffs"].items():
                                    row = {
                                        "changed_column": col_name,
                                        "dev_value": str(diff_values.get("dev")),
                                        "test_value": str(diff_values.get("test")),
                                    }
                                    for k in key_cols:
                                        row[k] = str(key_map.get(k))
                                    changed_rows_flat.append(row)

                            changed_df = pd.DataFrame(changed_rows_flat)
                            key_first_columns = key_cols + [
                                "changed_column",
                                "dev_value",
                                "test_value",
                            ]
                            changed_df = changed_df[key_first_columns]
                            st.dataframe(changed_df, use_container_width=True, hide_index=True)
                        else:
                            st.info("No changed rows.")

with tab_sql:
    st.markdown("### Step 3 - Generate DDL and sync SQL")
    if not st.session_state.connected or not st.session_state.selected_table:
        st.info("Complete Connect and choose a table in Compare step first.")
    else:
        selected_table = st.session_state.selected_table
        dev_cols = st.session_state.dev_schema.get(selected_table, {})
        test_cols = st.session_state.test_schema.get(selected_table, {})
        dev_table_def = st.session_state.get("dev_table_defs", {}).get(selected_table)
        col_diff_df = compare_table_columns(dev_cols, test_cols)

        st.markdown("#### Preview changes")
        summary = build_preview_summary(col_diff_df, st.session_state.data_diff_result)
        s1, s2, s3 = st.columns(3)
        s1.metric("Missing cols in TEST", summary["missing_in_test_columns"])
        s2.metric("Type differences", summary["different_type_columns"])
        s3.metric("Missing cols in DEV", summary["missing_in_dev_columns"])
        s4, s5, s6 = st.columns(3)
        s4.metric("Rows to INSERT", summary["rows_insert"])
        s5.metric("Rows to UPDATE", summary["rows_update"])
        s6.metric("Rows to DELETE", summary["rows_delete"])
        include_schema = st.checkbox("Include schema sync SQL (CREATE/ALTER)", value=True)
        include_data = st.checkbox("Include data sync SQL (INSERT/UPDATE/DELETE)", value=False)
        generate_clicked = st.button("Generate SQL script", type="primary")

        if generate_clicked:
            sql_lines = []
            if include_schema:
                sql_lines.extend(
                    generate_schema_sync_sql(
                        selected_table, dev_cols, test_cols, dev_table_def=dev_table_def
                    )
                )
            if include_data and st.session_state.data_diff_result and st.session_state.selected_keys:
                sql_lines.extend(
                    generate_data_sync_sql(
                        selected_table,
                        st.session_state.data_diff_result,
                        st.session_state.selected_keys,
                        st.session_state.data_diff_result["common_columns"],
                    )
                )
            st.session_state.generated_sql = build_sql_blob(sql_lines)

        st.markdown("#### Generated SQL")
        st.code(st.session_state.generated_sql or "-- Click 'Generate SQL script' to create output.", language="sql")
        st.download_button(
            "Download SQL",
            data=st.session_state.generated_sql or "-- empty",
            file_name=f"sync_{selected_table.replace('.', '_')}.sql",
            mime="text/sql",
            use_container_width=True,
        )

        if st.session_state.data_diff_result:
            st.markdown("#### Sync Script Generator (DEV -> TEST)")
            st.caption("This script applies DEV state to TEST for the selected table.")
            if st.button("Generate DEV -> TEST sync script"):
                sync_sql = generate_data_sync_sql(
                    selected_table,
                    st.session_state.data_diff_result,
                    st.session_state.selected_keys,
                    st.session_state.data_diff_result["common_columns"],
                )
                st.session_state.generated_sql = build_sql_blob(sync_sql)
                st.rerun()

with tab_sequence:
    st.markdown("### Step 4 - Compare and generate sequence SQL")
    if not st.session_state.connected:
        st.info("Open Connect step and load both schemas first.")
    else:
        dev_sequences = st.session_state.get("dev_sequences", {})
        test_sequences = st.session_state.get("test_sequences", {})
        missing_sequences_in_test = sorted(set(dev_sequences) - set(test_sequences))
        missing_sequences_in_dev = sorted(set(test_sequences) - set(dev_sequences))

        m1, m2, m3 = st.columns(3)
        m1.metric("Total DEV sequences", len(dev_sequences))
        m2.metric("Missing in TEST", len(missing_sequences_in_test))
        m3.metric("Missing in DEV", len(missing_sequences_in_dev))

        sq1, sq2 = st.columns(2)
        with sq1:
            with st.expander(
                f"Sequences missing in TEST ({len(missing_sequences_in_test)})",
                expanded=True,
            ):
                if missing_sequences_in_test:
                    st.dataframe(
                        pd.DataFrame({"sequence": missing_sequences_in_test}),
                        use_container_width=True,
                        hide_index=True,
                    )
                else:
                    st.caption("No missing sequences in TEST.")
        with sq2:
            with st.expander(
                f"Sequences missing in DEV ({len(missing_sequences_in_dev)})",
                expanded=True,
            ):
                if missing_sequences_in_dev:
                    st.dataframe(
                        pd.DataFrame({"sequence": missing_sequences_in_dev}),
                        use_container_width=True,
                        hide_index=True,
                    )
                else:
                    st.caption("No missing sequences in DEV.")

        if st.button("Generate sequence SQL", type="primary"):
            seq_sql_lines = generate_sequence_sync_sql(dev_sequences, test_sequences)
            st.session_state.generated_sequence_sql = build_sql_blob(seq_sql_lines)

        st.markdown("#### Generated sequence SQL")
        st.code(
            st.session_state.generated_sequence_sql
            or "-- Click 'Generate sequence SQL' to create output.",
            language="sql",
        )
        st.download_button(
            "Download sequence SQL",
            data=st.session_state.generated_sequence_sql or "-- empty",
            file_name="sync_sequences_dev_to_test.sql",
            mime="text/sql",
            use_container_width=True,
        )
