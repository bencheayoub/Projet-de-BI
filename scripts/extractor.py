"""
Data Extraction Module
Extracts data from SQL Server and MS Access sources.
"""

import pandas as pd
import os
import warnings
from config_connector import (
    create_sql_engine,
    create_access_connection,
    RAW_DIR
)

from sqlalchemy.exc import SAWarning

# Suppress SQLAlchemy warnings
warnings.filterwarnings("ignore", category=SAWarning)


def discover_access_tables():
    """Retrieve table names from Access database"""
    access_conn = create_access_connection()
    if not access_conn:
        return []

    cursor = access_conn.cursor()

    # Get all user tables (exclude system tables)
    tables = [
        row.table_name
        for row in cursor.tables(tableType='TABLE')
        if not row.table_name.startswith("MSys")
    ]

    cursor.close()
    access_conn.close()

    return tables


# ================================
# DIRECTORY SETUP
# ================================

os.makedirs(RAW_DIR, exist_ok=True)

# Tables to extract from both sources
EXTRACTION_TABLES = [
    'Orders',
    'Customers',
    'Employees',
    'Order Details',
    'shippers',
    'Territories',
    'EmployeeTerritories',
    'Region'
]


def normalize_table_name(name):
    """Standardize table names for comparison"""
    return name.lower().replace(" ", "").strip()


# ================================
# SQL SERVER EXTRACTION
# ================================

def extract_from_sql_server():
    """Extract data from SQL Server database"""
    sql_engine = create_sql_engine()
    if not sql_engine:
        print("SQL Server extraction skipped - connection unavailable")
        return

    print("Extracting from SQL Server...")

    for table in EXTRACTION_TABLES:
        try:
            # Handle tables with spaces in names
            sql_table_name = f"[{table}]" if " " in table else table

            df = pd.read_sql(f"SELECT * FROM {sql_table_name}", sql_engine)

            # Create output filename
            output_filename = f"sql_{table.replace(' ', '_').lower()}.csv"
            output_path = os.path.join(RAW_DIR, output_filename)

            df.to_csv(output_path, index=False)
            print(f"  SQL Server: {table:20} → {output_filename} ({df.shape[0]:,} rows)")

        except Exception as extraction_error:
            print(f"  SQL Server extraction error ({table}): {extraction_error}")


# ================================
# ACCESS DATABASE EXTRACTION
# ================================

def extract_from_access():
    """Extract data from MS Access database"""
    access_conn = create_access_connection()
    if not access_conn:
        print("Access database extraction skipped - connection unavailable")
        return

    # Discover available tables in Access
    access_tables = discover_access_tables()

    # Create normalized name mapping
    access_table_map = {
        normalize_table_name(t): t for t in access_tables
    }

    print("\nAccess database tables detected:")
    for table in access_tables:
        print(f"  - '{table}'")

    print("\nExtracting from Access database...")

    for table in EXTRACTION_TABLES:
        normalized_name = normalize_table_name(table)

        if normalized_name not in access_table_map:
            print(f"  Access: Table '{table}' not found in database")
            continue

        actual_table_name = access_table_map[normalized_name]

        try:
            df = pd.read_sql(f"SELECT * FROM [{actual_table_name}]", access_conn)

            output_filename = f"access_{table.replace(' ', '_').lower()}.csv"
            output_path = os.path.join(RAW_DIR, output_filename)

            df.to_csv(output_path, index=False)
            print(f"  Access: {table:20} → {output_filename} ({df.shape[0]:,} rows)")

        except Exception as access_error:
            print(f"  Access extraction error ({table}): {access_error}")

    access_conn.close()


# ================================
# EXTRACTION ORCHESTRATION
# ================================

def execute_extraction():
    """Main extraction function - runs both SQL Server and Access extractions"""
    print("\n" + "=" * 50)
    print("DATA EXTRACTION PROCESS")
    print("=" * 50)

    print("\n--- SQL Server Extraction ---")
    extract_from_sql_server()

    print("\n--- Access Database Extraction ---")
    extract_from_access()

    print("\n" + "-" * 30)
    print("EXTRACTION SUMMARY")
    print("-" * 30)

    for file in os.listdir(RAW_DIR):
        if file.endswith(".csv"):
            file_path = os.path.join(RAW_DIR, file)
            df = pd.read_csv(file_path)
            print(f"{file:30}: {len(df):,} rows")


if __name__ == "__main__":
    execute_extraction()