import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus
from collections import Counter


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


def compare_dataframes_keyless(dev_df: pd.DataFrame, test_df: pd.DataFrame):
    common_cols = [c for c in dev_df.columns if c in test_df.columns]
    if not common_cols:
        return {
            "common_cols": [],
            "only_dev": pd.DataFrame(),
            "only_test": pd.DataFrame(),
        }

    dev_norm = dev_df[common_cols].copy().fillna("<NULL>").astype(str)
    test_norm = test_df[common_cols].copy().fillna("<NULL>").astype(str)

    dev_records = [tuple(row) for row in dev_norm.to_records(index=False)]
    test_records = [tuple(row) for row in test_norm.to_records(index=False)]

    dev_counter = Counter(dev_records)
    test_counter = Counter(test_records)

    only_dev_records = []
    only_test_records = []

    for rec, cnt in (dev_counter - test_counter).items():
        only_dev_records.extend([rec] * cnt)

    for rec, cnt in (test_counter - dev_counter).items():
        only_test_records.extend([rec] * cnt)

    return {
        "common_cols": common_cols,
        "only_dev": pd.DataFrame(only_dev_records, columns=common_cols),
        "only_test": pd.DataFrame(only_test_records, columns=common_cols),
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

for side in ("dev", "test"):
    if f"{side}_tables" not in st.session_state:
        st.session_state[f"{side}_tables"] = []
    if f"{side}_schema" not in st.session_state:
        st.session_state[f"{side}_schema"] = {}
    if f"{side}_engine" not in st.session_state:
        st.session_state[f"{side}_engine"] = None
    if f"{side}_loaded_conn" not in st.session_state:
        st.session_state[f"{side}_loaded_conn"] = ""
    if f"{side}_error" not in st.session_state:
        st.session_state[f"{side}_error"] = None


def sync_from_dev():
    selected = st.session_state.get("dev_selected_table")
    if selected and selected in st.session_state.get("test_tables", []):
        st.session_state.test_selected_table = selected


def sync_from_test():
    selected = st.session_state.get("test_selected_table")
    if selected and selected in st.session_state.get("dev_tables", []):
        st.session_state.dev_selected_table = selected


def load_side(side: str, conn_str: str):
    raw_conn = (conn_str or "").strip()
    loaded_key = f"{side}_loaded_conn"

    if not raw_conn:
        st.session_state[f"{side}_tables"] = []
        st.session_state[f"{side}_schema"] = {}
        st.session_state[f"{side}_engine"] = None
        st.session_state[f"{side}_error"] = None
        st.session_state[loaded_key] = ""
        return

    if st.session_state.get(loaded_key) == raw_conn:
        return

    try:
        tables, schema, engine = safe_read_schema(raw_conn)
        st.session_state[f"{side}_tables"] = tables
        st.session_state[f"{side}_schema"] = schema
        st.session_state[f"{side}_engine"] = engine
        st.session_state[f"{side}_error"] = None
        st.session_state[loaded_key] = raw_conn
    except SQLAlchemyError as exc:
        st.session_state[f"{side}_tables"] = []
        st.session_state[f"{side}_schema"] = {}
        st.session_state[f"{side}_engine"] = None
        st.session_state[f"{side}_error"] = str(exc)
        st.session_state[loaded_key] = raw_conn


def extract_schemas(tables: list[str]) -> list[str]:
    schemas = sorted({t.split(".", 1)[0] for t in tables if "." in t})
    return schemas


def filter_tables(
    tables: list[str],
    schema_filter: str | None,
    text_filter: str | None,
) -> list[str]:
    result = tables
    if schema_filter and schema_filter != "(tum schemalar)":
        prefix = f"{schema_filter}."
        result = [t for t in result if t.startswith(prefix)]
    q = (text_filter or "").strip().lower()
    if q:
        result = [t for t in result if q in t.lower()]
    return result


conn_col1, conn_col2 = st.columns(2)
with conn_col1:
    st.markdown("### DEV Baglantisi")
    dev_conn = st.text_area(
        "DEV connection string",
        key="dev_conn_input",
        height=100,
        placeholder="Data Source=...;Initial Catalog=...;Integrated Security=True;TrustServerCertificate=True;",
    )
    if not (dev_conn or "").strip():
        st.markdown(
            "<span style='color:#dc2626;font-weight:600;'>DEV connection string bos birakilamaz.</span>",
            unsafe_allow_html=True,
        )
with conn_col2:
    st.markdown("### TEST Baglantisi")
    test_conn = st.text_area(
        "TEST connection string",
        key="test_conn_input",
        height=100,
        placeholder="Data Source=...;Initial Catalog=...;Integrated Security=True;TrustServerCertificate=True;",
    )
    if not (test_conn or "").strip():
        st.markdown(
            "<span style='color:#dc2626;font-weight:600;'>TEST connection string bos birakilamaz.</span>",
            unsafe_allow_html=True,
        )

btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 2])
with btn_col1:
    fetch_dev = st.button("DEV tablolarini getir", use_container_width=True)
with btn_col2:
    fetch_test = st.button("TEST tablolarini getir", use_container_width=True)
with btn_col3:
    fetch_both = st.button("Ikisini de getir", use_container_width=True)

if fetch_both:
    fetch_dev = True
    fetch_test = True

if fetch_dev:
    if not (dev_conn or "").strip():
        st.error("DEV connection string bos. Once DEV baglantisini gir.")
    else:
        load_side("dev", dev_conn)

if fetch_test:
    if not (test_conn or "").strip():
        st.error("TEST connection string bos. Once TEST baglantisini gir.")
    else:
        load_side("test", test_conn)

if st.session_state.dev_error:
    st.error(f"DEV baglanti hatasi: {st.session_state.dev_error}")
if st.session_state.test_error:
    st.error(f"TEST baglanti hatasi: {st.session_state.test_error}")

list_col1, list_col2 = st.columns(2)

with list_col1:
    st.subheader("DEV - Sema/Tablo")
    dev_tables = st.session_state.dev_tables
    if dev_tables:
        dev_schemas = extract_schemas(dev_tables)
        dev_schema_choice = st.selectbox(
            "DEV schema filtresi",
            options=["(tum schemalar)"] + dev_schemas,
            key="dev_schema_choice",
        )
        dev_text_filter = st.text_input(
            "DEV tablo ara (filtre)",
            key="dev_text_filter",
            placeholder="ornegin: order, invoice, dbo.",
        )
        dev_filtered_tables = filter_tables(dev_tables, dev_schema_choice, dev_text_filter)
        if not dev_filtered_tables:
            st.warning("Filtreye uyan DEV tablo bulunamadi.")
        else:
            if (
                "dev_selected_table" not in st.session_state
                or st.session_state.dev_selected_table not in dev_filtered_tables
            ):
                st.session_state.dev_selected_table = dev_filtered_tables[0]
            st.selectbox(
                "DEV tablo sec",
                options=dev_filtered_tables,
                key="dev_selected_table",
                on_change=sync_from_dev,
            )
        st.caption(f"Toplam tablo: {len(dev_tables)}")
    else:
        st.info("DEV tarafinda listelenecek tablo yok. (Butonla getir)")

with list_col2:
    st.subheader("TEST - Sema/Tablo")
    test_tables = st.session_state.test_tables
    if test_tables:
        test_schemas = extract_schemas(test_tables)
        test_schema_choice = st.selectbox(
            "TEST schema filtresi",
            options=["(tum schemalar)"] + test_schemas,
            key="test_schema_choice",
        )
        test_text_filter = st.text_input(
            "TEST tablo ara (filtre)",
            key="test_text_filter",
            placeholder="ornegin: order, invoice, dbo.",
        )
        test_filtered_tables = filter_tables(test_tables, test_schema_choice, test_text_filter)
        if not test_filtered_tables:
            st.warning("Filtreye uyan TEST tablo bulunamadi.")
        else:
            if (
                "test_selected_table" not in st.session_state
                or st.session_state.test_selected_table not in test_filtered_tables
            ):
                st.session_state.test_selected_table = test_filtered_tables[0]
            st.selectbox(
                "TEST tablo sec",
                options=test_filtered_tables,
                key="test_selected_table",
                on_change=sync_from_test,
            )
        st.caption(f"Toplam tablo: {len(test_tables)}")
    else:
        st.info("TEST tarafinda listelenecek tablo yok. (Butonla getir)")

if (
    st.session_state.get("dev_selected_table")
    and st.session_state.dev_selected_table in st.session_state.test_tables
):
    st.session_state.test_selected_table = st.session_state.dev_selected_table
elif (
    st.session_state.get("test_selected_table")
    and st.session_state.test_selected_table in st.session_state.dev_tables
):
    st.session_state.dev_selected_table = st.session_state.test_selected_table

selected_dev_table = st.session_state.get("dev_selected_table")
selected_test_table = st.session_state.get("test_selected_table")

compare_clicked = st.button("Tablolari karsilastir", type="primary")
if compare_clicked:
    st.session_state.last_compared = {
        "dev_table": selected_dev_table,
        "test_table": selected_test_table,
    }

last = st.session_state.get("last_compared") or {}
dev_to_compare = last.get("dev_table")
test_to_compare = last.get("test_table")

if dev_to_compare and test_to_compare:
    st.divider()
    st.subheader("Alan ve Tip Karsilastirmasi")
    st.caption(f"DEV: {dev_to_compare}  |  TEST: {test_to_compare}")

    dev_schema = st.session_state.dev_schema
    test_schema = st.session_state.test_schema
    dev_cols = dev_schema.get(dev_to_compare, {})
    test_cols = test_schema.get(test_to_compare, {})
    comp_df = compare_table_columns(dev_cols, test_cols)

    hide_same = st.checkbox("Ayni olan alanlari gizle", value=False)
    if hide_same:
        comp_df = comp_df[comp_df["status"] != "same"]

    st.dataframe(
        comp_df.style.apply(color_status, axis=1),
        use_container_width=True,
        hide_index=True,
    )

    if not dev_cols:
        st.warning("Secilen tablo DEV tarafinda bulunamadi.")
    if not test_cols:
        st.warning("Secilen tablo TEST tarafinda bulunamadi.")

    with st.expander("Veri farklari (opsiyonel)", expanded=False):
        enable_data_compare = st.checkbox("Verileri de karsilastir", key="enable_data_compare")
        if enable_data_compare:
            compare_mode = st.radio(
                "Karsilastirma modu",
                options=[
                    "Anahtar kolon ile satir eslestir",
                    "Kolondan bagimsiz (ham satir farki)",
                ],
            )
            limit = st.number_input("Maksimum satir (0 = tumu)", min_value=0, value=1000)

            available_key_cols = sorted(set(dev_cols.keys()) & set(test_cols.keys()))
            key_cols = []
            if compare_mode == "Anahtar kolon ile satir eslestir":
                key_cols = st.multiselect(
                    "Anahtar kolon(lar) (satir eslestirme icin)", available_key_cols
                )

            if st.button("Veri farklarini getir"):
                try:
                    dev_df = read_table_data(
                        st.session_state.dev_engine, dev_to_compare, int(limit)
                    )
                    test_df = read_table_data(
                        st.session_state.test_engine, test_to_compare, int(limit)
                    )

                    if compare_mode == "Anahtar kolon ile satir eslestir":
                        if not key_cols:
                            st.warning("Lutfen en az bir anahtar kolon secin.")
                            st.stop()
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
                    else:
                        keyless = compare_dataframes_keyless(dev_df, test_df)
                        if not keyless["common_cols"]:
                            st.warning("Ortak kolon yok, ham satir karsilastirmasi yapilamadi.")
                        else:
                            st.caption(
                                "Ham satir karsilastirmasi ortak kolonlar uzerinden yapildi."
                            )
                            st.markdown("#### Sadece Dev'de olan satirlar")
                            if keyless["only_dev"].empty:
                                st.info("Yok")
                            else:
                                st.dataframe(keyless["only_dev"], use_container_width=True)

                            st.markdown("#### Sadece Test'te olan satirlar")
                            if keyless["only_test"].empty:
                                st.info("Yok")
                            else:
                                st.dataframe(keyless["only_test"], use_container_width=True)
                except SQLAlchemyError as exc:
                    st.error(f"Veri okuma/karsilastirma hatasi: {exc}")
                except Exception as exc:  # pragma: no cover
                    st.error(f"Beklenmeyen hata: {exc}")
else:
    st.info("Karsilastirma icin en az bir tabloda secim yapin.")
