import os
import pandas as pd
import sqlite3
from task1 import clean_customers, clean_products, clean_transactions

def get_summary_statistics(conn, output_dir):
    
    query = """
    SELECT 
        (SELECT COUNT(*) FROM customers) AS total_customers,
        (SELECT COUNT(*) FROM products) AS total_products,
        (SELECT COUNT(*) FROM transactions) AS total_transactions;
    """
    summary_statistics = pd.read_sql_query(query, conn)
    summary_statistics_path = f"{output_dir}/record_count.csv"
    summary_statistics.to_csv(summary_statistics_path, index=False)


    query = """
        SELECT 
        COUNT(*) AS total_transactions,
        AVG(quantity) AS avg_quantity,
        MIN(quantity) AS min_quantity,
        MAX(quantity) AS max_quantity,
        AVG(price) AS avg_price,
        MIN(price) AS min_price,
        MAX(price) AS max_price,
        SUM(quantity * price) AS total_revenue
    FROM transactions;
    """
    summary_statistics = pd.read_sql_query(query, conn)
    summary_statistics = summary_statistics.T.reset_index()
    summary_statistics.columns = ['metric', 'value']
    # round values for better readability
    summary_statistics['value'] = summary_statistics['value'].apply(lambda x: round(x, 2) if isinstance(x, (int, float)) else x)
    summary_statistics_path = f"{output_dir}/avg_rng_cols.csv"
    summary_statistics.to_csv(summary_statistics_path, index=False)


    query = """
        SELECT 
        COUNT(DISTINCT product_id) AS total_products,
        AVG(cost_price) AS avg_cost_price,
        AVG(stock_quantity) AS avg_stock_quantity,
        SUM(stock_quantity) AS total_stock
        FROM products;
        """
    summary_statistics = pd.read_sql_query(query, conn)
    summary_statistics = summary_statistics.T.reset_index()
    summary_statistics.columns = ['metric', 'value']
    # round values for better readability
    summary_statistics['value'] = summary_statistics['value'].apply(lambda x: round(x, 2) if isinstance(x, (int, float)) else x)
    summary_statistics_path = f"{output_dir}/product_stock_summary.csv"
    summary_statistics.to_csv(summary_statistics_path, index=False)



    query = """
            SELECT 
        region,
        COUNT(*) AS num_customers
    FROM customers
    GROUP BY region
    ORDER BY num_customers DESC;
        """
    summary_statistics = pd.read_sql_query(query, conn)
    summary_statistics_path = f"{output_dir}/customer_region_summary.csv"
    summary_statistics.to_csv(summary_statistics_path, index=False)


def get_validation_reports(conn, output_dir):
    query = """
            SELECT customer_id, COUNT(*) AS cnt
        FROM customers
        GROUP BY customer_id
        HAVING cnt > 1;
        """
    summary_statistics = pd.read_sql_query(query, conn)
    summary_statistics_path = f"{output_dir}/customer_duplicates_validation.csv"
    summary_statistics.to_csv(summary_statistics_path, index=False)


    query = """
            SELECT product_id, COUNT(*) AS cnt
        FROM products
        GROUP BY product_id
        HAVING cnt > 1;
        """
    summary_statistics = pd.read_sql_query(query, conn)
    summary_statistics_path = f"{output_dir}/product_duplicates_validation.csv"
    summary_statistics.to_csv(summary_statistics_path, index=False)


    query = """
            SELECT transaction_id, COUNT(*) AS cnt
        FROM transactions
        GROUP BY transaction_id
        HAVING cnt > 1;
        """
    summary_statistics = pd.read_sql_query(query, conn)
    summary_statistics_path = f"{output_dir}/transaction_duplicates_validation.csv"
    summary_statistics.to_csv(summary_statistics_path, index=False)


    query = """
            SELECT COUNT(*) AS missing_regions
            FROM customers
            WHERE region IS NULL OR region = '';
        """
    summary_statistics = pd.read_sql_query(query, conn)
    summary_statistics_path = f"{output_dir}/missing_regions_validation.csv"
    summary_statistics.to_csv(summary_statistics_path, index=False)


    query = """
        SELECT COUNT(*) AS missing_price_qty
        FROM transactions
        WHERE price IS NULL OR quantity IS NULL;
    """
    summary_statistics = pd.read_sql_query(query, conn)
    summary_statistics_path = f"{output_dir}/missing_price_qty_validation.csv"
    summary_statistics.to_csv(summary_statistics_path, index=False)


    query = """
        SELECT *
        FROM products
        WHERE stock_quantity < 0;
    """
    summary_statistics = pd.read_sql_query(query, conn)
    summary_statistics_path = f"{output_dir}/negative_stock_validation.csv"
    summary_statistics.to_csv(summary_statistics_path, index=False)


output_dir = '../output/task5/'
os.makedirs(output_dir, exist_ok=True)

customers = pd.read_csv('../Preprocessed Datasets/customer_dataset.csv')
products = pd.read_csv('../Preprocessed Datasets/product_dataset.csv')
transactions = pd.read_csv('../Preprocessed Datasets/transaction_dataset.csv')

conn = sqlite3.connect(':memory:')
customers.to_sql('customers', conn, index=False)
products.to_sql('products', conn, index=False)
transactions.to_sql('transactions', conn, index=False)

get_validation_reports(conn, output_dir)

# Clean data
customers, cust_report = clean_customers(customers)
products, prod_report = clean_products(products)
transactions, trans_report = clean_transactions(transactions)

conn = sqlite3.connect(':memory:')
customers.to_sql('customers', conn, index=False)
products.to_sql('products', conn, index=False)
transactions.to_sql('transactions', conn, index=False)

get_summary_statistics(conn, output_dir)