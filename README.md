# File-to-Oracle-ETL-Pipeline
CSV/Excel → Oracle Data Loader is a Streamlit-based data ingestion tool that uploads CSV and Excel files into an Oracle database. It automatically sanitizes columns, infers data types, creates tables if missing, and performs efficient bulk inserts with logging and error handling.
📥 CSV / Excel → Oracle Data Loader

A Streamlit-based data ingestion tool that allows users to upload CSV or Excel files and seamlessly load them into an Oracle database.
The application automatically sanitizes column names, infers Oracle data types, creates tables if missing, and performs efficient bulk inserts with proper logging and error handling.

🚀 Features

Upload CSV & Excel files via a simple UI

Automatic column name sanitization (Oracle-safe identifiers)

Schema & data type inference for Oracle tables

Optional auto table creation if the target table does not exist

Bulk data insertion using executemany() for performance

Robust logging with rotation for debugging and audit trails

Clean, user-friendly Streamlit interface

🛠️ Tech Stack

Python

Streamlit

Pandas

Oracle Database (oracledb)

Logging (RotatingFileHandler)

🎯 Use Cases

Data ingestion & migration

ETL pipelines (file → database)

Rapid prototyping for data engineering workflows

Internal tools for analysts and engineers
