"""
Configuration and Database Connection Module
Centralizes settings and database connectivity for the ETL pipeline.
"""

import os
import sqlalchemy
import pyodbc

# ================================
# DATABASE CONFIGURATION
# ================================

SQL_SERVER_NAME = r'DESKTOP-ATCIINS\SQLEXPRESS'
SQL_DATABASE_NAME = 'Northwind'
ODBC_DRIVER = 'ODBC Driver 17 for SQL Server'
USE_TRUSTED_CONNECTION = 'yes'

ACCESS_DATABASE_NAME = 'Northwind 2012.accdb'

# ================================
# PROJECT DIRECTORY STRUCTURE
# ================================

# Base project directory
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_ROOT = os.path.join(PROJECT_ROOT, 'data')

# Access database full path
ACCESS_DB_PATH = os.path.join(DATA_ROOT, ACCESS_DATABASE_NAME)

# Data subdirectories
RAW_DIR = os.path.join(DATA_ROOT, 'raw')
STAGING_DIR = os.path.join(DATA_ROOT, 'staging')
WAREHOUSE_DIR = os.path.join(DATA_ROOT, 'warehouse')


def create_sql_engine():
    """Create SQLAlchemy engine for SQL Server connection"""
    try:
        connection_string = (
            f"mssql+pyodbc://@{SQL_SERVER_NAME}/{SQL_DATABASE_NAME}?"
            f"driver={ODBC_DRIVER}&trusted_connection={USE_TRUSTED_CONNECTION}"
        )
        engine = sqlalchemy.create_engine(connection_string)
        return engine
    except Exception as connection_error:
        print(f"SQL Server connection failed: {connection_error}")
        return None


def create_access_connection():
    """Establish pyodbc connection to MS Access database"""
    try:
        # Connection string for .accdb files
        conn_string = fr"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={ACCESS_DB_PATH};"
        connection = pyodbc.connect(conn_string)
        return connection
    except Exception as access_error:
        print(f"Access database connection failed: {access_error}")
        print("Ensure 'Microsoft Access Database Engine' is installed.")
        return None