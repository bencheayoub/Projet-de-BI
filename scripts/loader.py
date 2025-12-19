"""
Warehouse Loading Module
Transforms staging data into final warehouse tables and generates schema.
"""

import pandas as pd
import os
from config_connector import STAGING_DIR, WAREHOUSE_DIR

# Create warehouse directory if missing
os.makedirs(WAREHOUSE_DIR, exist_ok=True)


def create_ddl_schema(table_dict):
    """Generate SQL DDL statements from dataframe schemas"""
    ddl_statements = []

    # Primary key definitions per table
    primary_keys = {
        'DimDate': 'sk_date',
        'DimClient': 'sk_client',
        'DimEmployee': 'sk_employee',
        'FactSales': 'fact_id'
    }

    print("Generating SQL schema...")

    for tbl_name, df in table_dict.items():
        column_definitions = []

        for col_name, dtype in df.dtypes.items():
            # Map pandas types to SQL types
            if "int" in str(dtype):
                sql_type = "INT"
            elif "float" in str(dtype):
                sql_type = "DECIMAL(10,2)"
            elif "datetime" in str(dtype):
                sql_type = "DATE"
            else:
                sql_type = "VARCHAR(255)"

            # Add PRIMARY KEY constraint
            if col_name == primary_keys.get(tbl_name):
                sql_type += " PRIMARY KEY"

            column_definitions.append(f"    {col_name} {sql_type}")

        ddl_statements.append(
            f"CREATE TABLE {tbl_name} (\n" +
            ",\n".join(column_definitions) +
            "\n);\n"
        )

    # Write schema to file
    schema_path = os.path.join(WAREHOUSE_DIR, "schema.sql")
    with open(schema_path, "w") as schema_file:
        schema_file.write("\n".join(ddl_statements))

    print(f"Schema saved to: {schema_path}")


def load_staging_to_warehouse():
    """Main loading function: transforms staging files to warehouse tables"""
    print("\n" + "="*50)
    print("STAGING TO WAREHOUSE LOAD PROCESS")
    print("="*50)

    # Mapping: staging files -> final warehouse tables
    # Note: DimProduct intentionally excluded as requested
    staging_to_warehouse_map = {
        'cleaned_date': 'DimDate',
        'cleaned_clients': 'DimClient',
        'cleaned_employees': 'DimEmployee',
        'cleaned_sales': 'FactSales'
    }

    loaded_datasets = {}

    for staging_name, warehouse_name in staging_to_warehouse_map.items():
        staging_path = os.path.join(STAGING_DIR, f"{staging_name}.csv")

        if os.path.exists(staging_path):
            # Load staging data
            df = pd.read_csv(staging_path)
            loaded_datasets[warehouse_name] = df

            # Save to warehouse in multiple formats
            warehouse_csv = os.path.join(WAREHOUSE_DIR, f"{warehouse_name}.csv")
            warehouse_parquet = os.path.join(WAREHOUSE_DIR, f"{warehouse_name}.parquet")

            df.to_csv(warehouse_csv, index=False)
            df.to_parquet(warehouse_parquet, index=False)

            print(f"[TRANSFORMED] {staging_name:20} → {warehouse_name} ({len(df):,} rows)")
        else:
            print(f"[MISSING] Required staging file: {staging_name}")

    # Generate SQL schema if data loaded
    if loaded_datasets:
        create_ddl_schema(loaded_datasets)

    print("\n" + "-"*30)
    print("LOAD PROCESS COMPLETE ✓")
    print("Data Warehouse is ready for use.")
    print("-"*30)


if __name__ == "__main__":
    load_staging_to_warehouse()