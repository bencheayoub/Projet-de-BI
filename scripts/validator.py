"""
Data Warehouse Validation Module
Checks existence, completeness, and data quality of warehouse tables.
"""

import pandas as pd
import os
from config_connector import WAREHOUSE_DIR


def validate_missing_values(df, table_name):
    """Check for null values in a dataframe"""
    null_count = df.isnull().sum().sum()
    if null_count > 0:
        print(f"[WARNING] {table_name}: Contains {null_count} missing values (may be expected for optional fields).")
    else:
        print(f"[OK] {table_name}: No missing values detected.")


def execute_warehouse_validation():
    """Main validation function for data warehouse"""
    print("\n" + "=" * 50)
    print("DATA WAREHOUSE VALIDATION")
    print("=" * 50)

    # Tables expected in the warehouse (DimProduct removed per requirements)
    warehouse_tables = ['FactSales', 'DimDate', 'DimClient', 'DimEmployee']
    validation_passed = True

    for table in warehouse_tables:
        file_path = os.path.join(WAREHOUSE_DIR, f"{table}.parquet")

        # Check table existence
        if not os.path.exists(file_path):
            print(f"[ERROR] Table missing: {table}")
            validation_passed = False
            continue

        try:
            # Load and inspect table
            df = pd.read_parquet(file_path)

            if df.empty:
                print(f"[ERROR] Table empty: {table}")
                validation_passed = False
            else:
                print(f"[LOADED] {table}: {len(df):,} records")
                validate_missing_values(df, table)

        except Exception as error:
            print(f"[ERROR] Failed to read {table}: {error}")
            validation_passed = False

    # Final validation result
    print("\n" + "-" * 30)
    if validation_passed:
        print("VALIDATION RESULT: PASSED ✓")
    else:
        print("VALIDATION RESULT: FAILED ✗")
    print("-" * 30)


if __name__ == "__main__":
    execute_warehouse_validation()