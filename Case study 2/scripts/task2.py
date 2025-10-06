import os
import pandas as pd
from task1 import clean_customers, clean_products, clean_transactions

# 3) Transformation: centralized transactions table --------------------------------
def build_central_transactions(trans, cust, prod):
    df = trans.merge(cust, how='left', on='customer_id', suffixes=('','_cust'))
    df = df.merge(prod, how='left', on='product_id', suffixes=('','_prod'))
    # Compose customer name
    df['customer_name'] = df['first_name'].fillna('') + ' ' + df['last_name'].fillna('')
    df['customer_name'] = df['customer_name'].str.strip()
    # Fill missing product_name or category with 'Unknown' if present
    df['product_name'] = df['product_name'].fillna('Unknown')
    df['category'] = df['category'].fillna('Unknown')
    # Compute profit: (price - cost_price) * quantity
    df['cost_price'] = pd.to_numeric(df['cost_price'], errors='coerce').fillna(0.0)
    df['profit'] = (df['price'] - df['cost_price']) * df['quantity']
    # Rearrange columns per specification
    central_cols = ['transaction_id','customer_id','customer_name','product_id','product_name',
                    'category','quantity','price','transaction_date','store_id','profit']
    df_central = df[central_cols].copy()
    return df_central

output_dir = '../output/task2/'
os.makedirs(output_dir, exist_ok=True)

customers = pd.read_csv('../Preprocessed Datasets/customer_dataset.csv')
products = pd.read_csv('../Preprocessed Datasets/product_dataset.csv')
transactions = pd.read_csv('../Preprocessed Datasets/transaction_dataset.csv')

# Clean data
customers, cust_report = clean_customers(customers)
products, prod_report = clean_products(products)
transactions, trans_report = clean_transactions(transactions)

df_central = build_central_transactions(transactions, customers, products)
df_central.to_csv(os.path.join(output_dir, "cleaned_transactions.csv"), index=False)

# Aggregated stock
stock_agg = products.groupby(['product_id','product_name','category'], as_index=False).agg(
    total_stock=('stock_quantity','sum'),
    avg_cost_price=('cost_price','mean')
)
stock_agg.to_csv(os.path.join(output_dir, "aggregated_stock.csv"), index=False)

