CREATE TABLE DimDate (
    full_date VARCHAR(255),
    sk_date INT PRIMARY KEY,
    year INT,
    month INT,
    month_name VARCHAR(255),
    quarter INT
);

CREATE TABLE DimClient (
    sk_client INT PRIMARY KEY,
    bk_customer_id VARCHAR(255),
    company_name VARCHAR(255),
    city VARCHAR(255),
    country VARCHAR(255),
    region VARCHAR(255)
);

CREATE TABLE DimEmployee (
    sk_employee INT PRIMARY KEY,
    bk_employee_id DECIMAL(10,2),
    Employee_name VARCHAR(255),
    title VARCHAR(255),
    city VARCHAR(255),
    country VARCHAR(255),
    sales_region VARCHAR(255),
    territories VARCHAR(255)
);

CREATE TABLE FactSales (
    fact_id INT PRIMARY KEY,
    bk_order_id INT,
    sk_client DECIMAL(10,2),
    sk_employee INT,
    sk_date INT,
    quantity DECIMAL(10,2),
    unit_price DECIMAL(10,2),
    discount DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    delivery_status VARCHAR(255)
);
