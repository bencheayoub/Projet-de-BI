"""
Data Transformation Module
Processes raw extracted data into cleaned dimensional and fact tables.
"""

import pandas as pd
import os
import glob
from config_connector import RAW_DIR, STAGING_DIR

# Ensure staging directory exists
os.makedirs(STAGING_DIR, exist_ok=True)


def standardize_column_names(df):
    """Clean and standardize dataframe column names"""
    if df is not None:
        df.columns = [
            col.lower()
            .strip()
            .replace(' ', '')
            .replace('_', '')
            for col in df.columns
        ]
    return df


def load_source_data(table_name, primary_key_cols):
    """Load and merge multiple source files for a given table"""
    dataframes = []
    search_pattern = os.path.join(RAW_DIR, f"*_{table_name}.csv")
    source_files = glob.glob(search_pattern)

    print(f"Loading '{table_name}': Found {len(source_files)} source file(s)")

    for file_path in source_files:
        try:
            df = pd.read_csv(file_path)
            df = standardize_column_names(df)
            dataframes.append(df)
        except Exception as load_error:
            print(f"Failed to load {file_path}: {load_error}")

    if not dataframes:
        return pd.DataFrame()

    # Combine all source files
    combined_df = pd.concat(dataframes, ignore_index=True)

    # Deduplicate based on primary keys
    clean_primary_keys = [key.lower() for key in primary_key_cols]

    if set(clean_primary_keys).issubset(combined_df.columns):
        combined_df = combined_df.drop_duplicates(
            subset=clean_primary_keys,
            keep='first'
        )

    return combined_df


def create_debug_master(orders_df, details_df, customers_df, employees_df, products_df):
    """Create comprehensive master table for debugging purposes"""
    master = pd.merge(details_df, orders_df, on='orderid', how='inner')

    if not customers_df.empty:
        master = pd.merge(master, customers_df, on='customerid', how='left', suffixes=('', '_cust'))

    if not employees_df.empty:
        master = pd.merge(master, employees_df, on='employeeid', how='left', suffixes=('', '_emp'))

    if not products_df.empty:
        master = pd.merge(master, products_df, on='productid', how='left', suffixes=('', '_prod'))

    return master


def create_date_dimension(orders_df):
    """Generate DimDate dimension table from order dates"""
    print("Creating DimDate dimension...")

    if orders_df.empty:
        return pd.DataFrame()

    orders_df['orderdate'] = orders_df['orderdate'].astype(str)

    try:
        dates = pd.to_datetime(
            orders_df['orderdate'],
            format='mixed',
            errors='coerce'
        ).dropna().unique()
    except:
        dates = pd.to_datetime(
            orders_df['orderdate'],
            infer_datetime_format=True,
            errors='coerce'
        ).dropna().unique()

    date_dim = pd.DataFrame({'full_date': dates})
    date_dim = date_dim.sort_values('full_date')

    # Generate date attributes
    date_dim['sk_date'] = date_dim['full_date'].dt.strftime('%Y%m%d').astype(int)
    date_dim['year'] = date_dim['full_date'].dt.year
    date_dim['month'] = date_dim['full_date'].dt.month
    date_dim['month_name'] = date_dim['full_date'].dt.month_name()
    date_dim['quarter'] = date_dim['full_date'].dt.quarter

    return date_dim


def create_client_dimension(customers_df):
    """Generate DimClient dimension table"""
    print("Creating DimClient dimension...")

    if customers_df.empty:
        return pd.DataFrame()

    client_dim = customers_df.copy()

    # Rename and clean columns
    client_dim = client_dim.rename(columns={
        'customerid': 'bk_customer_id',
        'companyname': 'company_name'
    })

    client_dim['bk_customer_id'] = (
        client_dim['bk_customer_id']
        .astype(str)
        .str.upper()
        .str.strip()
    )

    # Ensure required geography columns exist
    for geo_col in ['city', 'country', 'region']:
        if geo_col not in client_dim.columns:
            client_dim[geo_col] = 'Unknown'

    # Add surrogate key
    client_dim['sk_client'] = range(1, len(client_dim) + 1)

    # Select final columns
    target_columns = ['sk_client', 'bk_customer_id', 'company_name', 'city', 'country', 'region']
    available_columns = [col for col in target_columns if col in client_dim.columns]

    return client_dim[available_columns]


def add_territory_info(employee_dim, emp_terr_df, territories_df, regions_df):
    """Enrich employee dimension with territory and region information"""
    if emp_terr_df.empty or territories_df.empty:
        employee_dim['territories'] = 'Unknown'
        employee_dim['sales_region'] = 'Unknown'
        return employee_dim

    # Merge territory information
    merged = pd.merge(emp_terr_df, territories_df, on='territoryid', how='left')

    # Add region information if available
    if not regions_df.empty and 'regionid' in merged.columns:
        merged = pd.merge(merged, regions_df, on='regionid', how='left')
        region_col_name = 'regiondescription'
    else:
        merged['regiondescription'] = 'Unknown'
        region_col_name = 'regiondescription'

    # Clean text fields
    if 'territorydescription' in merged.columns:
        merged['territorydescription'] = (
            merged['territorydescription'].astype(str).str.strip()
        )

    if region_col_name in merged.columns:
        merged[region_col_name] = (
            merged[region_col_name].astype(str).str.strip()
        )

    # Aggregate territories per employee
    territories_per_employee = (
        merged.groupby('employeeid')['territorydescription']
        .apply(lambda x: ', '.join(x))
        .reset_index(name='territories')
    )

    # Aggregate regions per employee
    regions_per_employee = (
        merged.groupby('employeeid')[region_col_name]
        .apply(lambda x: ', '.join(set(x)))
        .reset_index(name='sales_region')
    )

    # Merge aggregated data back to employee dimension
    employee_dim = pd.merge(
        employee_dim,
        territories_per_employee,
        left_on='bk_employee_id',
        right_on='employeeid',
        how='left'
    )

    employee_dim = pd.merge(
        employee_dim,
        regions_per_employee,
        left_on='bk_employee_id',
        right_on='employeeid',
        how='left'
    )

    # Fill missing values
    employee_dim['territories'] = employee_dim['territories'].fillna('No Territory')
    employee_dim['sales_region'] = employee_dim['sales_region'].fillna('No Region')

    # Clean up temporary columns
    return employee_dim.drop(columns=['employeeid_x', 'employeeid_y'], errors='ignore')


def create_employee_dimension(employees_df, emp_terr_df, territories_df, regions_df):
    """Generate DimEmployee dimension table with HR enrichment"""
    print("Creating DimEmployee dimension...")

    if employees_df.empty:
        return pd.DataFrame()

    employee_dim = employees_df.copy()

    # Rename columns
    employee_dim = employee_dim.rename(columns={
        'employeeid': 'bk_employee_id',
        'firstname': 'first_name',
        'lastname': 'last_name'
    })

    # Create full name field
    employee_dim['Employee_name'] = (
            employee_dim['first_name'].astype(str) +
            ' ' +
            employee_dim['last_name'].astype(str)
    )

    # Add surrogate key
    employee_dim['sk_employee'] = range(1, len(employee_dim) + 1)

    # Enrich with territory information
    employee_dim = add_territory_info(
        employee_dim,
        emp_terr_df,
        territories_df,
        regions_df
    )

    # Select final columns
    target_columns = [
        'sk_employee',
        'bk_employee_id',
        'Employee_name',
        'title',
        'city',
        'country',
        'sales_region',
        'territories'
    ]

    available_columns = [col for col in target_columns if col in employee_dim.columns]

    return employee_dim[available_columns]


def create_sales_fact(orders_df, details_df, client_dim, employee_dim):
    """Generate FactSales fact table"""
    print("Creating FactSales fact table...")

    if orders_df.empty or details_df.empty:
        return pd.DataFrame()

    # Merge order details with order headers
    sales_fact = pd.merge(details_df, orders_df, on='orderid', how='inner')

    # Identify price column
    if 'unitprice_x' in sales_fact.columns:
        price_column = 'unitprice_x'
    elif 'unitprice' in sales_fact.columns:
        price_column = 'unitprice'
    else:
        return pd.DataFrame()

    # Calculate total amount
    sales_fact['total_amount'] = (
                                         sales_fact[price_column] *
                                         sales_fact['quantity']
                                 ) * (1 - sales_fact['discount'])

    # Determine delivery status
    sales_fact['delivery_status'] = sales_fact['shippeddate'].apply(
        lambda x: 'Delivered' if pd.notnull(x) and str(x).strip() != '' else 'Not Delivered'
    )

    # Clean customer ID for joining
    if 'customerid' in sales_fact.columns:
        sales_fact['customerid'] = (
            sales_fact['customerid']
            .astype(str)
            .str.upper()
            .str.strip()
        )

    # Link to client dimension
    if not client_dim.empty:
        sales_fact = pd.merge(
            sales_fact,
            client_dim[['bk_customer_id', 'sk_client']],
            left_on='customerid',
            right_on='bk_customer_id',
            how='left'
        )

    # Link to employee dimension
    if not employee_dim.empty:
        sales_fact = pd.merge(
            sales_fact,
            employee_dim[['bk_employee_id', 'sk_employee']],
            left_on='employeeid',
            right_on='bk_employee_id',
            how='left'
        )

    # Create date key
    sales_fact['orderdate'] = sales_fact['orderdate'].astype(str)

    try:
        sales_fact['dt'] = pd.to_datetime(
            sales_fact['orderdate'],
            format='mixed',
            errors='coerce'
        )
    except:
        sales_fact['dt'] = pd.to_datetime(
            sales_fact['orderdate'],
            infer_datetime_format=True,
            errors='coerce'
        )

    sales_fact['sk_date'] = (
        sales_fact['dt']
        .dt.strftime('%Y%m%d')
        .fillna(19000101)
        .astype(int)
    )

    # Final column renaming
    sales_fact = sales_fact.rename(columns={
        'orderid': 'bk_order_id',
        price_column: 'unit_price'
    })

    # Add fact surrogate key
    sales_fact['fact_id'] = range(1, len(sales_fact) + 1)

    # Select final columns
    final_columns = [
        'fact_id',
        'bk_order_id',
        'sk_client',
        'sk_employee',
        'sk_date',
        'quantity',
        'unit_price',
        'discount',
        'total_amount',
        'delivery_status'
    ]

    available_columns = [col for col in final_columns if col in sales_fact.columns]

    return sales_fact[available_columns]


def execute_transformation():
    """Main transformation orchestration function"""
    print("\n" + "=" * 50)
    print("DATA TRANSFORMATION PROCESS")
    print("=" * 50)

    # Load source data
    orders = load_source_data('orders', ['orderid'])
    details = load_source_data('order_details', ['orderid', 'productid'])
    customers = load_source_data('customers', ['customerid'])
    employees = load_source_data('employees', ['employeeid'])

    # Load additional HR data for employee enrichment
    emp_territories = load_source_data('employeeterritories', ['employeeid', 'territoryid'])
    territories = load_source_data('territories', ['territoryid'])
    regions = load_source_data('region', ['regionid'])

    # Create dimension tables
    dim_date = create_date_dimension(orders)
    dim_client = create_client_dimension(customers)
    dim_employee = create_employee_dimension(
        employees,
        emp_territories,
        territories,
        regions
    )

    # Create fact table
    fact_sales = create_sales_fact(orders, details, dim_client, dim_employee)

    # Save to staging
    print("\n" + "-" * 30)
    print("SAVING STAGING FILES")
    print("-" * 30)

    dim_date.to_csv(os.path.join(STAGING_DIR, 'cleaned_date.csv'), index=False)
    dim_client.to_csv(os.path.join(STAGING_DIR, 'cleaned_clients.csv'), index=False)
    dim_employee.to_csv(os.path.join(STAGING_DIR, 'cleaned_employees.csv'), index=False)
    fact_sales.to_csv(os.path.join(STAGING_DIR, 'cleaned_sales.csv'), index=False)

    print("\nTRANSFORMATION COMPLETE ✓")


if __name__ == "__main__":
    execute_transformation()