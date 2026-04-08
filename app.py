import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus


st.set_page_config(page_title="DB Diff Tool", layout="wide")


COLOR_ONLY_DEV = "#16a34a"  # green
COLOR_ONLY_TEST = "#dc2626"  # red
COLOR_SAME = "#6b7280"  # gray
COLOR_TYPE_DIFF = "#f59e0b"  # amber


def safe_read_schema(conn_str: str):
    engine = create_engine(normalize_connection_string(conn_str))
    inspector = inspect(engine)
    tables = []
    schema = {}

    for schema_name in inspector.get_schema_names():
        if schema_name in ("INFORMATION_SCHEMA", "sys"):
            continue
        for table in inspector.get_table_names(schema=schema_name):
            full_table_name = f"{schema_name}.{table}"
            tables.append(full_table_name)
            cols = inspector.get_columns(table, schema=schema_name)
            schema[full_table_name] = {
                col["name"]: str(col.get("type", "UNKNOWN")).upper() for col in cols
            }

    return tables, schema, engine


def normalize_connection_string(conn_str: str) -> str:
    raw = (conn_str or "").strip()
    if not raw:
        return raw

    # If user already provides a SQLAlchemy URL, keep it as-is.
    if "://" in raw:
        return raw

    # Support ADO.NET style SQL Server strings like:
    # Data Source=...;Initial Catalog=...;Integrated Security=True;TrustServerCertificate=True;
    parts = {}
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

    integrated_security = (parts.get("integrated security") or "").lower() in (
        "true",
        "yes",
        "sspi",
    )

    params = [f"DRIVER={{{parts.get('driver', 'ODBC Driver 17 for SQL Server')}}}"]
    if integrated_security:
        params.append("Trusted_Connection=yes")
    else:
        user = parts.get("user id") or parts.get("uid")
        password = parts.get("password") or parts.get("pwd")
        if user and password:
            params.append(f"UID={user}")
            params.append(f"PWD={password}")

    if (parts.get("trustservercertificate") or "").lower() in ("true", "yes"):
        params.append("TrustServerCertificate=yes")

    odbc_str = f"SERVER={server};DATABASE={database};" + ";".join(params) + ";"
    return f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"


def compare_table_columns(dev_cols: dict, test_cols: dict) -> pd.DataFrame:
    all_columns = sorted(set(dev_cols.keys()) | set(test_cols.keys()))
    rows = []

    for col_name in all_columns:
        dev_type = dev_cols.get(col_name)
        test_type = test_cols.get(col_name)

        if dev_type and not test_type:
            status = "only_dev"
            color = COLOR_ONLY_DEV
        elif test_type and not dev_type:
            status = "only_test"
            color = COLOR_ONLY_TEST
        elif dev_type == test_type:
            status = "same"
            color = COLOR_SAME
        else:
            status = "type_diff"
            color = COLOR_TYPE_DIFF

        rows.append(
            {
                "column": col_name,
                "dev_type": dev_type or "-",
                "test_type": test_type or "-",
                "status": status,
                "color": color,
            }
        )

    return pd.DataFrame(rows)


def quote_ident(name: str) -> str:
    return f"[{name.replace(']', ']]')}]"


def read_table_data(engine, table_name: str, limit: int) -> pd.DataFrame:
    if "." in table_name:
        schema_name, simple_table_name = table_name.split(".", 1)
        qualified_table = f"{quote_ident(schema_name)}.{quote_ident(simple_table_name)}"
    else:
        qualified_table = quote_ident(table_name)

    query = text(f"SELECT * FROM {qualified_table}")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    if limit > 0:
        return df.head(limit)
    return df


def compare_dataframes(dev_df: pd.DataFrame, test_df: pd.DataFrame, key_cols: list[str]):
    dev_pref = dev_df.copy().set_index(key_cols, drop=False)
    test_pref = test_df.copy().set_index(key_cols, drop=False)

    common_idx = dev_pref.index.intersection(test_pref.index)
    only_dev_idx = dev_pref.index.difference(test_pref.index)
    only_test_idx = test_pref.index.difference(dev_pref.index)

    changed_rows = []
    common_cols = [c for c in dev_df.columns if c in test_df.columns]

    for idx in common_idx:
        dev_row = dev_pref.loc[idx, common_cols]
        test_row = test_pref.loc[idx, common_cols]

        # Handle potential duplicate keys by flattening.
        if isinstance(dev_row, pd.DataFrame) or isinstance(test_row, pd.DataFrame):
            continue

        diffs = {}
        for col in common_cols:
            dev_val = dev_row[col]
            test_val = test_row[col]
            if pd.isna(dev_val) and pd.isna(test_val):
                continue
            if str(dev_val) != str(test_val):
                diffs[col] = {"dev": dev_val, "test": test_val}

        if diffs:
            changed_rows.append({"key": idx, "diffs": diffs})

    return {
        "only_dev": dev_pref.loc[only_dev_idx].reset_index(drop=True),
        "only_test": test_pref.loc[only_test_idx].reset_index(drop=True),
        "changed": changed_rows,
    }


def color_status(row):
    return [f"background-color: {row['color']}; color: white"] * len(row)


def build_table_presence_options(dev_tables: list[str], test_tables: list[str]):
    all_tables = sorted(set(dev_tables) | set(test_tables))
    options = []
    for table_name in all_tables:
        in_dev = table_name in dev_tables
        in_test = table_name in test_tables
        if in_dev and in_test:
            label = f"{table_name} (iki tarafta)"
        elif in_dev:
            label = f"{table_name} (sadece DEV)"
        else:
            label = f"{table_name} (sadece TEST)"
        options.append((label, table_name))
    return options


st.title("DB Diff Tool (Dev vs Test)")
st.caption(
    "Iki connection string ile tablo/alan tipi farklarini ve istenirse veri farklarini gosterir."
)

with st.sidebar:
    st.header("Baglanti")
    dev_conn = st.text_input("Dev connection string", type="password")
    test_conn = st.text_input("Test connection string", type="password")
    connect = st.button("Baglan ve getir")

if "connected" not in st.session_state:
    st.session_state.connected = False

if connect:
    try:
        dev_tables, dev_schema, dev_engine = safe_read_schema(dev_conn)
        test_tables, test_schema, test_engine = safe_read_schema(test_conn)
        st.session_state.connected = True
        st.session_state.dev_tables = dev_tables
        st.session_state.test_tables = test_tables
        st.session_state.dev_schema = dev_schema
        st.session_state.test_schema = test_schema
        st.session_state.dev_engine = dev_engine
        st.session_state.test_engine = test_engine
        st.success("Baglanti basarili.")
    except SQLAlchemyError as exc:
        st.session_state.connected = False
        st.error(f"Baglanti hatasi: {exc}")

if st.session_state.connected:
    dev_tables = st.session_state.dev_tables
    test_tables = st.session_state.test_tables
    dev_schema = st.session_state.dev_schema
    test_schema = st.session_state.test_schema

    table_options = build_table_presence_options(dev_tables, test_tables)
    selected_label = st.selectbox("Tablo sec", [item[0] for item in table_options])
    selected_table = next(
        table_name for label, table_name in table_options if label == selected_label
    )

    dev_cols = dev_schema.get(selected_table, {})
    test_cols = test_schema.get(selected_table, {})
    comp_df = compare_table_columns(dev_cols, test_cols)

    hide_same = st.checkbox("Ayni olan alanlari gizle", value=False)
    if hide_same:
        comp_df = comp_df[comp_df["status"] != "same"]

    st.subheader("Alan ve Tip Karsilastirmasi")
    st.dataframe(
        comp_df.style.apply(color_status, axis=1),
        use_container_width=True,
        hide_index=True,
    )

    if not dev_cols:
        st.warning("Bu tablo DEV tarafinda yok.")
    if not test_cols:
        st.warning("Bu tablo TEST tarafinda yok.")

    with st.expander("Veri karsilastirmasi (opsiyonel)", expanded=False):
        enable_data_compare = st.checkbox("Verileri de karsilastir")
        if enable_data_compare:
            available_key_cols = sorted(set(dev_cols.keys()) & set(test_cols.keys()))
            key_cols = st.multiselect(
                "Anahtar kolon(lar) (satir eslestirme icin)", available_key_cols
            )
            limit = st.number_input("Maksimum satir (0 = tumu)", min_value=0, value=1000)

            if st.button("Veri farklarini getir"):
                if not key_cols:
                    st.warning("Lutfen en az bir anahtar kolon secin.")
                else:
                    try:
                        dev_df = read_table_data(
                            st.session_state.dev_engine, selected_table, int(limit)
                        )
                        test_df = read_table_data(
                            st.session_state.test_engine, selected_table, int(limit)
                        )
                        result = compare_dataframes(dev_df, test_df, key_cols)

                        st.markdown("#### Sadece Dev'de olan satirlar")
                        if result["only_dev"].empty:
                            st.info("Yok")
                        else:
                            st.dataframe(result["only_dev"], use_container_width=True)

                        st.markdown("#### Sadece Test'te olan satirlar")
                        if result["only_test"].empty:
                            st.info("Yok")
                        else:
                            st.dataframe(result["only_test"], use_container_width=True)

                        st.markdown("#### Her ikisinde var ama farkli olan satirlar")
                        if not result["changed"]:
                            st.info("Yok")
                        else:
                            for item in result["changed"][:200]:
                                st.write(f"Anahtar: {item['key']}")
                                st.json(item["diffs"])
                    except SQLAlchemyError as exc:
                        st.error(f"Veri okuma/karsilastirma hatasi: {exc}")
                    except Exception as exc:  # pragma: no cover
                        st.error(f"Beklenmeyen hata: {exc}")
else:
    st.info("Dev ve Test connection string girip 'Baglan ve getir' butonuna basin.")
