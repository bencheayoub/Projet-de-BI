# Northwind ETL Data Warehouse Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline for building a data warehouse from the Northwind database, combining data from SQL Server and MS Access sources into a dimensional model for business intelligence and analytics.

## 📋 Project Overview

This project extracts data from multiple source systems (SQL Server and MS Access), transforms it into a star schema data warehouse, and validates the results. The pipeline produces clean, analysis-ready dimension and fact tables optimized for business intelligence reporting.

## 🏗️ Architecture

### Data Flow
```
SQL Server + MS Access → Raw CSV Files → Staging Tables → Data Warehouse
```

### Directory Structure
```
northwind-etl/
├── data/                          # All data files
│   ├── raw/                       # Extracted source data
│   ├── staging/                   # Cleaned intermediate data
│   ├── warehouse/                 # Final data warehouse tables
│   ├── Northwind 2012.accdb       # MS Access database
│   └── sqlserver.sql              # SQL Server schema
├── figures/                       # Analysis visualizations
├── notebooks/                     # Jupyter notebooks for exploration
├── scripts/                       # ETL pipeline scripts
│   ├── config_connector.py        # Database connections
│   ├── extractor.py              # Data extraction
│   ├── transformer.py            # Data transformation
│   ├── loader.py                 # Warehouse loading
│   ├── validator.py              # Data quality validation
│   └── elt.py                    # Main orchestration
└── requirements.txt              # Python dependencies
```

## 📊 Data Warehouse Schema

### Dimension Tables
- **DimDate**: Date attributes for temporal analysis
- **DimClient**: Customer information with geography
- **DimEmployee**: Employee details with territory assignments

### Fact Table
- **FactSales**: Sales transactions with relationships to all dimensions

*(Note: DimProduct intentionally excluded as per requirements)*

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- SQL Server with Northwind database
- MS Access with Northwind 2012 database
- ODBC Driver 17 for SQL Server
- Microsoft Access Database Engine

### Installation & Setup

#### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/northwind-etl.git
cd northwind-etl
```

#### 2. Create and Activate Virtual Environment

**Windows:**
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
venv\Scripts\activate

# Upgrade pip
python -m pip install --upgrade pip
```

**macOS/Linux:**
```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Upgrade pip
python -m pip install --upgrade pip
```

#### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

#### 4. Configure Database Connections
Edit `scripts/config_connector.py` with your database settings:
```python
SQL_SERVER_NAME = r'DESKTOP-ATCIINS\SQLEXPRESS'  # Update to your SQL Server instance
SQL_DATABASE_NAME = 'Northwind'
ACCESS_DATABASE_NAME = 'Northwind 2012.accdb'    # Ensure this file is in the data/ directory
```

### Running the ETL Pipeline

#### Method 1: Complete ETL Pipeline
```bash
cd scripts
python elt.py
```

#### Method 2: Individual Stages
```bash
# 1. Extraction only
python extractor.py

# 2. Transformation only
python transformer.py

# 3. Loading only
python loader.py

# 4. Validation only
python validator.py
```

### Running Jupyter Notebook for Analysis

#### 1. Install Jupyter in Virtual Environment (if not already installed)
```bash
pip install jupyter
```

#### 2. Launch Jupyter Notebook
```bash
# Make sure you're in the project root directory
cd northwind-etl

# Start Jupyter Notebook
jupyter notebook
```

#### 3. Access Notebooks
- Open `notebooks/exploration.ipynb` for initial data exploration
- Open `notebooks/dashboard_analysis.ipynb` for BI dashboard creation

#### 4. Deactivate Virtual Environment (when done)
```bash
deactivate
```

## 📝 Quick Start Commands Summary

```bash
# One-time setup
git clone https://github.com/yourusername/northwind-etl.git
cd northwind-etl
python -m venv venv
venv\Scripts\activate  # Windows
# OR source venv/bin/activate  # Mac/Linux
pip install -r requirements.txt
pip install jupyter  # For analysis notebooks

# Run ETL pipeline
cd scripts
python elt.py

# Run analysis notebooks
cd ..
jupyter notebook
```

## 🎯 Common Workflows

### 1. **Full ETL Pipeline Execution**
```bash
# Activate environment
venv\Scripts\activate

# Run complete pipeline
cd scripts
python elt.py

# Return to project root
cd ..
```

### 2. **ETL + Analysis Workflow**
```bash
# Step 1: Run ETL
venv\Scripts\activate
cd scripts
python elt.py

# Step 2: Analyze results
cd ..
jupyter notebook
# Open notebooks/dashboard_analysis.ipynb
```

### 3. **Development/Testing Workflow**
```bash
venv\Scripts\activate

# Test extraction
cd scripts
python extractor.py

# Test transformation
python transformer.py

# Test specific functions in Python interpreter
python -c "import transformer; help(transformer.create_date_dimension)"
```

## 🔧 Script Details

### 1. **config_connector.py**
- Central configuration management
- Database connection setup for SQL Server and MS Access
- Directory path definitions

### 2. **extractor.py**
- Extracts data from SQL Server (via SQLAlchemy)
- Extracts data from MS Access (via pyodbc)
- Saves raw data as CSV files
- Handles table name normalization

### 3. **transformer.py**
- Cleans and standardizes column names
- Merges data from multiple sources
- Creates dimension and fact tables
- Handles data deduplication
- Enriches employee data with territory information

### 4. **loader.py**
- Transforms staging data to final warehouse format
- Exports data as CSV and Parquet
- Generates SQL DDL schema automatically

### 5. **validator.py**
- Checks table existence and completeness
- Validates data quality (missing values)
- Provides pass/fail validation report

### 6. **elt.py**
- Main orchestration script
- Coordinates all ETL phases
- Measures execution time
- Provides unified logging

## 📈 Analysis & Visualization

The project includes:
- **Exploration Notebook** (`notebooks/exploration.ipynb`): Initial data analysis
- **Dashboard Analysis** (`notebooks/dashboard_analysis.ipynb`): BI dashboard creation
- **Pre-generated Visualizations** (`figures/`): Key business insights including:
  - Revenue trends
  - Delivery performance by territory
  - Employee performance metrics
  - Client delivery analysis
  - 3D OLAP visualization

## 🧪 Data Quality Features

- **Source Validation**: Checks for missing source files
- **Null Value Detection**: Identifies missing data
- **Referential Integrity**: Validates dimension-fact relationships
- **Duplicate Handling**: Removes duplicate records based on primary keys
- **Type Safety**: Ensures consistent data types across sources

## 📝 Output Files

### Warehouse Tables (in `data/warehouse/`)
- `DimDate.csv/.parquet` - Date dimension
- `DimClient.csv/.parquet` - Client dimension
- `DimEmployee.csv/.parquet` - Employee dimension
- `FactSales.csv/.parquet` - Sales fact table
- `schema.sql` - Auto-generated SQL DDL schema

### Generated Schema Example
```sql
CREATE TABLE DimDate (
    full_date DATE,
    sk_date INT PRIMARY KEY,
    year INT,
    month INT,
    month_name VARCHAR(255),
    quarter INT
);
```

## 🛠️ Technical Features

- **Dual Source Integration**: Combines SQL Server and MS Access data
- **Incremental Design**: Modular scripts for easy maintenance
- **Error Handling**: Comprehensive exception handling and logging
- **Format Flexibility**: Outputs in CSV, Parquet, and SQL formats
- **Performance Optimized**: Efficient pandas operations for large datasets
- **Data Validation**: Multi-stage quality checks

## 🔍 Key Business Insights

The data warehouse enables analysis of:
- Sales performance by time, client, and employee
- Delivery efficiency across territories
- Regional sales trends
- Employee territory coverage
- Client purchasing patterns

## ❓ Troubleshooting

### Common Issues:

1. **Database Connection Failed**
   - Verify SQL Server is running
   - Check ODBC driver installation
   - Ensure MS Access file is in `data/` directory

2. **Virtual Environment Issues**
   ```bash
   # If activation fails (Windows):
   Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
   
   # Recreate environment if corrupted:
   deactivate
   rmdir /s venv
   python -m venv venv
   ```

3. **Missing Dependencies**
   ```bash
   # Reinstall from requirements
   pip install --force-reinstall -r requirements.txt
   ```

4. **Jupyter Kernel Issues**
   ```bash
   # Install ipykernel in virtual environment
   pip install ipykernel
   
   # Add virtual environment to Jupyter
   python -m ipykernel install --user --name=venv --display-name="Python (venv)"
   ```


## 📄 License

MIT License

Copyright (c) 2024 Benchetioui ahmed ayoub

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.