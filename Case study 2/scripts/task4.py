import os
import pandas as pd
import sqlite3
from task1 import clean_customers, clean_products, clean_transactions


def sql_queries(output_dir, conn):
    # Transactions per store
    sql_transactions_per_store = """
        SELECT store_id, COUNT(*) AS num_transactions
        FROM transactions
        GROUP BY store_id
        ORDER BY num_transactions DESC;
        """
    transactions_per_store = pd.read_sql_query(sql_transactions_per_store, conn)
    transactions_per_store_path = f"{output_dir}/transactions_per_store_sql.csv"
    transactions_per_store.to_csv(transactions_per_store_path, index=False)

    # Products with profit margin <10%:
    sql_low_margin = """
    SELECT p.product_id, p.product_name,
        p.cost_price,
        AVG(t.price) AS avg_price,
        ((AVG(t.price) - p.cost_price) / AVG(t.price)) * 100 AS margin_percent
    FROM products p
    JOIN transactions t ON p.product_id = t.product_id
    GROUP BY p.product_id
    HAVING margin_percent < 10
    ORDER BY margin_percent ASC;
    """
    low_margin_products = pd.read_sql_query(sql_low_margin, conn)
    low_margin_products_path = f"{output_dir}/low_margin_products_sql.csv"
    low_margin_products.to_csv(low_margin_products_path, index=False)

    # Top 3 regions by revenue
    sql_top_regions = """
    SELECT c.region, SUM(t.price * t.quantity) AS revenue
    FROM transactions t
    JOIN customers c ON t.customer_id = c.customer_id
    GROUP BY c.region
    ORDER BY revenue DESC
    LIMIT 3;
    """
    top_regions = pd.read_sql_query(sql_top_regions, conn)
    top_regions_path = f"{output_dir}/top_regions_sql.csv"
    top_regions.to_csv(top_regions_path, index=False)


output_dir = '../output/task4/'
os.makedirs(output_dir, exist_ok=True)

customers = pd.read_csv('../Preprocessed Datasets/customer_dataset.csv')
products = pd.read_csv('../Preprocessed Datasets/product_dataset.csv')
transactions = pd.read_csv('../Preprocessed Datasets/transaction_dataset.csv')

# Clean data
customers, cust_report = clean_customers(customers)
products, prod_report = clean_products(products)
transactions, trans_report = clean_transactions(transactions)

conn = sqlite3.connect(':memory:')
customers.to_sql('customers', conn, index=False)
products.to_sql('products', conn, index=False)
transactions.to_sql('transactions', conn, index=False)

sql_queries(output_dir, conn)