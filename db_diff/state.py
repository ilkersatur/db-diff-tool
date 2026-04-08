import streamlit as st


DEFAULT_STATE = {
    "connected": False,
    "active_step": 0,
    "selected_table": None,
    "table_search": "",
    "show_only_differences": False,
    "row_limit": 1000,
    "column_search": "",
    "selected_keys": [],
    "data_compare_enabled": False,
    "data_diff_result": None,
    "generated_sql": "",
}


def init_state() -> None:
    for key, default_value in DEFAULT_STATE.items():
        if key not in st.session_state:
            st.session_state[key] = default_value


def reset_runtime_outputs() -> None:
    st.session_state.data_diff_result = None
    st.session_state.generated_sql = ""

