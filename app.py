from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy.exc import SQLAlchemyError

from db_diff.ddl import build_preview_summary, generate_data_sync_sql, generate_schema_sync_sql
from db_diff.services import (
    compare_dataframes,
    compare_table_columns,
    make_engine,
    read_schema,
    read_table_data,
)
from db_diff.state import init_state, reset_runtime_outputs
from db_diff.ui import STATUS_LABELS, inject_style, render_status_legend, style_diff_table

st.set_page_config(page_title="Database Diff Studio", page_icon=":material/compare:", layout="wide")
init_state()
inject_style()


@st.cache_resource(show_spinner=False)
def get_engine(conn_str: str):
    return make_engine(conn_str)


@st.cache_data(show_spinner=False, ttl=300)
def get_schema_snapshot(conn_str: str):
    return read_schema(get_engine(conn_str))


@st.cache_data(show_spinner=False, ttl=120)
def get_table_snapshot(conn_str: str, table_name: str, limit: int):
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


st.title("Database Diff Studio")
st.caption("Compare DEV and TEST schemas/data and generate sync-ready SQL scripts.")

with st.sidebar:
    st.subheader("Workflow")
    st.caption("1) Connect -> 2) Compare -> 3) Generate SQL")

    st.subheader("Filters")
    st.checkbox("Show only differences", key="show_only_differences")
    st.text_input("Search table/column", key="table_search", placeholder="e.g. dbo.Users or Email")
    st.number_input("Row limit (0 = all rows)", min_value=0, max_value=200000, key="row_limit")

    st.subheader("Actions")
    if st.button("Reset selections", use_container_width=True):
        reset_runtime_outputs()
        st.session_state.selected_table = None
        st.session_state.selected_keys = []
        st.rerun()

tab_connect, tab_compare, tab_sql = st.tabs(["Connect", "Compare", "DDL & Sync Script"])

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

    connect_clicked = st.button("Connect and load schemas", type="primary")
    if connect_clicked:
        if not dev_conn or not test_conn:
            st.error("Both DEV and TEST connection strings are required.")
        else:
            bar = st.progress(0, text="Starting connection checks...")
            try:
                with st.spinner("Connecting DEV..."):
                    bar.progress(20, text="Connecting DEV")
                    dev_tables, dev_schema = get_schema_snapshot(dev_conn)
                with st.spinner("Connecting TEST..."):
                    bar.progress(60, text="Connecting TEST")
                    test_tables, test_schema = get_schema_snapshot(test_conn)
                bar.progress(100, text="Connection complete")

                st.session_state.connected = True
                st.session_state.dev_tables = dev_tables
                st.session_state.test_tables = test_tables
                st.session_state.dev_schema = dev_schema
                st.session_state.test_schema = test_schema
                st.success("Connections successful. Move to Compare step.")
            except SQLAlchemyError as exc:
                st.session_state.connected = False
                st.error(f"Database error while connecting: {exc}")
            except Exception as exc:  # pragma: no cover
                st.session_state.connected = False
                st.error(f"Unexpected connection error: {exc}")

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
        search_term = st.session_state.table_search.lower().strip()
        if search_term:
            table_df = table_df[table_df["table"].str.lower().str.contains(search_term)]
        if st.session_state.show_only_differences:
            table_df = table_df[table_df["status"] != "matching"]

        m1, m2, m3 = st.columns(3)
        m1.metric("Tables in both", int((table_df["status"] == "matching").sum()))
        m2.metric("Missing in TEST", int((table_df["status"] == "missing_in_test").sum()))
        m3.metric("Missing in DEV", int((table_df["status"] == "missing_in_dev").sum()))
        render_status_legend()

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
                                    int(st.session_state.row_limit),
                                )
                                bar.progress(50, text="Reading TEST table data...")
                                test_df = get_table_snapshot(
                                    st.session_state.test_conn,
                                    selected_table,
                                    int(st.session_state.row_limit),
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
                            for item in result["changed"][:200]:
                                st.write(f"Key: {item['key']}")
                                st.json(item["diffs"])
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
        include_data = st.checkbox("Include data sync SQL (INSERT/UPDATE/DELETE)", value=True)
        generate_clicked = st.button("Generate SQL script", type="primary")

        if generate_clicked:
            sql_lines = []
            if include_schema:
                sql_lines.extend(generate_schema_sync_sql(selected_table, dev_cols, test_cols))
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
