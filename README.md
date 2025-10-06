# ETL Case Study 2 - Data Pipeline Documentation

## Overview

This project implements a comprehensive ETL (Extract, Transform, Load) pipeline for processing e-commerce transactional data. The pipeline handles customer, product, and transaction datasets, applying data cleaning, transformation, and analytical processing to generate business insights.

## Project Structure

```
Case study 2/
├── Case Study Document.pdf          # Original requirements and specifications
├── Documentation.txt                # Detailed technical documentation
├── README.md                       # This file
├── Preprocessed Datasets/          # Input data files
│   ├── customer_dataset.csv        # Customer information (ID, name, region, signup date)
│   ├── product_dataset.csv         # Product catalog (ID, name, category, cost, stock)
│   └── transaction_dataset.csv     # Transaction records (ID, customer, product, quantity, price)
├── scripts/                        # Python ETL scripts
│   ├── task1.py                   # Data cleaning and validation
│   ├── task2.py                   # Data transformation and integration
│   ├── task3.py                   # Analytics and anomaly detection
│   ├── task4.py                   # SQL-based analysis
│   └── task5.py                   # Data quality validation and reporting
└── output/                        # Generated outputs organized by task
    ├── task2/
    ├── task3/
    ├── task4/
    └── task5/
```

## Pipeline Tasks

### Task 1: Data Cleaning ([task1.py](scripts/task1.py))
**Purpose**: Clean and standardize raw datasets

**Key Functions**:
- `clean_customers()` - Removes duplicates, standardizes names and regions, handles missing values
- `clean_products()` - Validates product data, handles negative stock quantities
- `clean_transactions()` - Removes invalid transactions, ensures numeric data integrity

**Cleaning Rules**:
- **Customers**: Drop duplicates by customer_id, title-case names, replace missing regions with 'Unknown'
- **Products**: Drop duplicates by product_id, convert negative stock to 0, ensure numeric cost_price
- **Transactions**: Drop duplicates by transaction_id, remove rows missing quantity/price, standardize dates

### Task 2: Data Transformation ([task2.py](scripts/task2.py))
**Purpose**: Create integrated datasets and perform transformations

**Key Functions**:
- `build_central_transactions()` - Joins customers, products, and transactions into unified fact table

**Outputs** → [`output/task2/`](output/task2/):
- [`cleaned_transactions.csv`](output/task2/cleaned_transactions.csv) - Central fact table with all entities joined
- [`aggregated_stock.csv`](output/task2/aggregated_stock.csv) - Product-level stock aggregations

**Derived Fields**:
- `customer_name` = first_name + last_name
- `revenue` = price × quantity  
- `profit` = (price - cost_price) × quantity

### Task 3: Analytics & Anomaly Detection ([task3.py](scripts/task3.py))
**Purpose**: Generate business insights and detect data anomalies

**Key Functions**:
- `analytics_report()` - Produces comprehensive business analytics
- `detect_anomalies()` - Identifies statistical outliers using Z-score analysis

**Outputs** → [`output/task3/`](output/task3/):
- [`transactions_per_region.csv`](output/task3/transactions_per_region.csv) - Transaction volume by geographic region
- [`revenue_by_category.csv`](output/task3/revenue_by_category.csv) - Revenue breakdown by product category
- [`top_products.csv`](output/task3/top_products.csv) - Top 5 products by revenue performance
- [`customers_with_missing_before.csv`](output/task3/customers_with_missing_before.csv) - Data quality assessment
- [`quantity_anomalies.csv`](output/task3/quantity_anomalies.csv) - Statistical outliers in quantity data
- [`price_anomalies.csv`](output/task3/price_anomalies.csv) - Statistical outliers in price data

### Task 4: SQL Analysis ([task4.py](scripts/task4.py))
**Purpose**: Perform complex analytical queries using SQL

**Key Functions**:
- `sql_queries()` - Executes business intelligence queries against in-memory SQLite database

**Outputs** → [`output/task4/`](output/task4/):
- [`transactions_per_store_sql.csv`](output/task4/transactions_per_store_sql.csv) - Store-level transaction analysis
- [`low_margin_products_sql.csv`](output/task4/low_margin_products_sql.csv) - Products with profit margin <10%
- [`top_regions_sql.csv`](output/task4/top_regions_sql.csv) - Top 3 regions by revenue using SQL aggregation

### Task 5: Data Quality & Validation ([task5.py](scripts/task5.py))
**Purpose**: Comprehensive data quality assessment and validation reporting

**Key Functions**:
- `get_summary_statistics()` - Generates statistical summaries and record counts
- `get_validation_reports()` - Creates detailed data quality validation reports

**Outputs** → [`output/task5/`](output/task5/):

**Summary Statistics**:
- [`record_count.csv`](output/task5/record_count.csv) - Record counts across all tables
- [`avg_rng_cols.csv`](output/task5/avg_rng_cols.csv) - Statistical summaries for numeric columns
- [`customer_region_summary.csv`](output/task5/customer_region_summary.csv) - Customer distribution by region
- [`product_stock_summary.csv`](output/task5/product_stock_summary.csv) - Product inventory summaries

**Validation Reports**:
- [`customer_duplicates_validation.csv`](output/task5/customer_duplicates_validation.csv) - Customer duplicate detection
- [`product_duplicates_validation.csv`](output/task5/product_duplicates_validation.csv) - Product duplicate detection  
- [`transaction_duplicates_validation.csv`](output/task5/transaction_duplicates_validation.csv) - Transaction duplicate detection
- [`missing_regions_validation.csv`](output/task5/missing_regions_validation.csv) - Missing region data analysis
- [`negative_stock_validation.csv`](output/task5/negative_stock_validation.csv) - Negative stock quantity detection
- [`missing_price_qty_validation.csv`](output/task5/missing_price_qty_validation.csv) - Missing price/quantity validation

## Data Processing Flow

```
Raw CSV Files → Cleaning (Task 1) → Integration (Task 2) → Analytics (Task 3) → SQL Analysis (Task 4) → Validation (Task 5)
       ↓              ↓                    ↓                    ↓                    ↓                    ↓
  Input Data    Cleaned Data      Central Fact Table    Business Insights    Complex Queries    Quality Reports
```

## Key Features

### Data Quality Assurance
- **Duplicate Detection**: Comprehensive duplicate removal across all datasets
- **Missing Value Handling**: Systematic approach to null/missing data
- **Data Type Validation**: Ensures numeric fields are properly formatted
- **Date Standardization**: Converts all dates to ISO format (YYYY-MM-DD)
- **Anomaly Detection**: Z-score based statistical outlier identification

### Business Intelligence
- **Revenue Analysis**: Product category and regional revenue breakdowns
- **Customer Insights**: Geographic distribution and transaction patterns  
- **Product Performance**: Top performers and low-margin product identification
- **Inventory Management**: Stock level analysis and aggregations

### Technical Implementation
- **Modular Design**: Each task is independent and reusable
- **Comprehensive Logging**: Detailed processing logs and data lineage
- **Scalable Architecture**: Pandas-based processing suitable for larger datasets
- **SQL Integration**: In-memory SQLite for complex analytical queries

## Requirements

- Python 3.7+
- pandas
- numpy
- sqlite3

## Usage

Execute tasks individually or in sequence:

```bash
# Individual task execution - go inside the scripts folder and run the below commands
python task1.py  # Data cleaning
python task2.py  # Data transformation  
python task3.py  # Analytics & anomaly detection
python task4.py  # SQL-based analysis
python task5.py  # Data quality validation

```

## Output Directory Structure

All processed outputs are organized by task for easy navigation and analysis:

- **task2/**: Cleaned and integrated datasets
- **task3/**: Business analytics and anomaly reports  
- **task4/**: SQL-based analytical results
- **task5/**: Data quality validation and summary statistics

## Data Quality Standards

The pipeline implements strict data quality standards:

- Zero tolerance for duplicate records
- Standardized text formatting (Title Case)
- Missing value replacement strategies
- Date format standardization
- Numeric data validation and coercion
- Statistical anomaly detection and reporting

This ETL pipeline provides a robust foundation for e-commerce data analysis, ensuring data quality while generating actionable business insights.
