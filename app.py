import os
import io
import re
import logging
from logging.handlers import RotatingFileHandler
from typing import Optional, Union, Dict, List

import streamlit as st
import pandas as pd
import oracledb

# ======================= LOGGING =======================

def get_logger(name="app"):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)

    fh = RotatingFileHandler("app.log", maxBytes=2_000_000, backupCount=3)
    fh.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)
    logger.propagate = False
    return logger

logger = get_logger()

# ======================= HELPERS =======================

MAX_IDENTIFIER_LEN = 30

def sanitize_identifier(name: str) -> str:
    name = str(name).strip().upper()
    name = re.sub(r"[^A-Z0-9_]", "_", name)
    if re.match(r"^\d", name):
        name = "_" + name
    return name[:MAX_IDENTIFIER_LEN]

def sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [sanitize_identifier(c) for c in df.columns]
    return df

def infer_oracle_type(col: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(col):
        return "NUMBER"
    if pd.api.types.is_float_dtype(col):
        return "NUMBER"
    if pd.api.types.is_datetime64_any_dtype(col):
        return "TIMESTAMP"
    return "VARCHAR2(4000)"

def build_create_table_sql(schema, table, df):
    cols = [
        f"{c} {infer_oracle_type(df[c])}"
        for c in df.columns
    ]
    full = f"{schema}.{table}" if schema else table
    return f"CREATE TABLE {full} (\n" + ",\n".join(cols) + "\n)"

# ======================= ORACLE =======================

def get_connection(user, password, host, port, service):
    dsn = oracledb.makedsn(host, port, service_name=service)
    return oracledb.connect(user=user, password=password, dsn=dsn)

def table_exists(conn, schema, table):
    owner = schema.upper() if schema else conn.username.upper()
    sql = """
        SELECT COUNT(*) FROM ALL_TABLES
        WHERE OWNER = :o AND TABLE_NAME = :t
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"o": owner, "t": table})
        return cur.fetchone()[0] > 0

def create_table(conn, schema, table, df):
    sql = build_create_table_sql(schema, table, df)
    with conn.cursor() as cur:
        cur.execute(sql)

def insert_dataframe(conn, schema, table, df):
    df = df.where(pd.notnull(df), None)
    cols = list(df.columns)
    binds = ",".join([f":{i+1}" for i in range(len(cols))])
    full = f"{schema}.{table}" if schema else table

    sql = f"INSERT INTO {full} ({','.join(cols)}) VALUES ({binds})"

    with conn.cursor() as cur:
        cur.executemany(sql, df.values.tolist())

# ======================= FILE =======================

def read_file(uploaded_file):
    uploaded_file.seek(0)
    if uploaded_file.name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    return pd.read_excel(uploaded_file)

# ======================= STREAMLIT UI =======================

st.set_page_config("CSV/Excel → Oracle Loader", "📥")
st.title("📥 CSV / Excel → Oracle Loader")

st.sidebar.header("Oracle Connection")

host = st.sidebar.text_input("Host", "localhost")
port = st.sidebar.number_input("Port", value=1521)
service = st.sidebar.text_input("Service", "FREEPDB1")
user = st.sidebar.text_input("User")
password = st.sidebar.text_input("Password", type="password")
schema = st.sidebar.text_input("Schema (optional)")

uploaded_file = st.file_uploader("Upload CSV or Excel", ["csv", "xlsx"])

table_name = st.text_input("Destination Table")

create_if_missing = st.checkbox("Create table if not exists", value=True)

if uploaded_file:
    df = sanitize_columns(read_file(uploaded_file))
    st.success(f"Loaded {len(df)} rows")
    st.dataframe(df.head())

if st.button("🚀 Upload to Oracle"):
    try:
        conn = get_connection(user, password, host, port, service)
        exists = table_exists(conn, schema, table_name)

        if not exists:
            if not create_if_missing:
                st.error("Table does not exist")
                st.stop()
            create_table(conn, schema, table_name, df)
            conn.commit()
            st.success("Table created")

        insert_dataframe(conn, schema, table_name, df)
        conn.commit()
        st.success("Data inserted successfully ✅")

    except Exception as e:
        logger.exception("Upload failed")
        st.error(str(e))
