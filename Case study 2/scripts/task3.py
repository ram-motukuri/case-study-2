import os
import pandas as pd
import numpy as np
from task1 import clean_customers, clean_products, clean_transactions

def detect_anomalies(df, col, z_thresh=3.0):
    vals = df[col].astype(float)
    mu = vals.mean()
    sigma = vals.std(ddof=0)
    # Handle sigma==0
    if sigma == 0 or np.isnan(sigma):
        return pd.DataFrame([], columns=df.columns), mu, sigma
    z = (vals - mu) / sigma
    anomalies = df[np.abs(z) > z_thresh].copy()
    anomalies['_zscore'] = z[np.abs(z) > z_thresh]
    return anomalies


def analytics_report(transactions, customers, products, output_dir):
    # Total transactions per region (we need region from customers joined via central)
    trans_with_region = transactions.merge(customers[['customer_id','region']], how='inner', on='customer_id')
    transactions_per_region = trans_with_region.groupby('region', as_index=False).agg(total_transactions=('transaction_id','count'))
    transactions_per_region.to_csv(os.path.join(output_dir, "transactions_per_region.csv"), index=False)

    # Total revenue by product category
    trans_with_product = transactions.merge(products[['product_id','product_name', 'category']], how='inner', on='product_id')

    trans_with_product['revenue'] = trans_with_product['price'] * trans_with_product['quantity']
    revenue_by_category = trans_with_product.groupby('category', as_index=False).agg(total_revenue=('revenue','sum'))
    revenue_by_category.to_csv(os.path.join(output_dir, "revenue_by_category.csv"), index=False)

    # Top 5 products by revenue
    top_products = trans_with_product.groupby(['product_id','product_name'], as_index=False).agg(
        revenue=('revenue','sum'), total_qty=('quantity','sum')
    ).sort_values('revenue', ascending=False).head(5)

    top_products.to_csv(os.path.join(output_dir, "top_products.csv"), index=False)

    # Customers with missing data BEFORE cleaning (we saved raw customers earlier)
    customers_with_missing_before = customers[customers.isnull().any(axis=1)].copy()
    customers_with_missing_before.to_csv(os.path.join(output_dir, "customers_with_missing_before.csv"), index=False)

    qty_anoms = detect_anomalies(transactions, 'quantity')
    price_anoms = detect_anomalies(transactions, 'price')

    qty_anoms.to_csv(os.path.join(output_dir, "quantity_anomalies.csv"), index=False)
    price_anoms.to_csv(os.path.join(output_dir, "price_anomalies.csv"), index=False)


output_dir = '../output/task3/'
os.makedirs(output_dir, exist_ok=True)

customers = pd.read_csv('../Preprocessed Datasets/customer_dataset.csv')
products = pd.read_csv('../Preprocessed Datasets/product_dataset.csv')
transactions = pd.read_csv('../Preprocessed Datasets/transaction_dataset.csv')

# Clean data
customers, cust_report = clean_customers(customers)
products, prod_report = clean_products(products)
transactions, trans_report = clean_transactions(transactions)

analytics_report(transactions, customers, products, output_dir)
