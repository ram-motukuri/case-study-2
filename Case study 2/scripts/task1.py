import pandas as pd


def clean_customers(df):
    """
    Cleaning rules for customers:
    - Drop exact duplicates (based on customer_id)
    - Standardize text: names title-cased, region title-cased
    - Replace missing regions with "Unknown"
    - Standardize signup_date to YYYY-MM-DD (coerce errors)
    """
    df = df.copy()
    before = len(df)
    df = df.drop_duplicates(subset=['customer_id']).reset_index(drop=True)
    after = len(df)
    # Normalize names and region
    df['first_name'] = df['first_name'].astype(str).str.strip().str.title()
    df['last_name'] = df['last_name'].astype(str).str.strip().str.title()
    df['region'] = df['region'].replace({None: None})
    df['region'] = df['region'].fillna('Unknown').astype(str).str.strip().str.title()
    # Standardize date
    df['signup_date'] = pd.to_datetime(df['signup_date'], errors='coerce').dt.strftime('%Y-%m-%d')
    return df, {'before_count': before, 'after_count': after}

def clean_products(df):
    """
    Cleaning rules for products:
    - Drop duplicates by product_id
    - Normalize product_name (strip, title)
    - Standardize category to title case
    - Convert negative stock to 0
    - Ensure numeric cost_price
    """
    df = df.copy()
    before = len(df)
    df = df.drop_duplicates(subset=['product_id']).reset_index(drop=True)
    after = len(df)
    df['product_name'] = df['product_name'].astype(str).str.strip().str.title()
    df['category'] = df['category'].astype(str).str.strip().str.title()
    df['stock_quantity'] = pd.to_numeric(df['stock_quantity'], errors='coerce').fillna(0).astype(int)
    df['stock_quantity'] = df['stock_quantity'].clip(lower=0)
    df['cost_price'] = pd.to_numeric(df['cost_price'], errors='coerce').fillna(0.0)
    return df, {'before_count': before, 'after_count': after}

def clean_transactions(df):
    """
    Cleaning rules for transactions:
    - Drop duplicates by transaction_id
    - Drop rows with missing quantity or price
    - Ensure numeric types for quantity and price
    - Standardize transaction_date to YYYY-MM-DD
    """
    df = df.copy()
    before = len(df)
    df = df.drop_duplicates(subset=['transaction_id']).reset_index(drop=True)
    # Drop rows with missing quantity or price
    df = df[~(df['quantity'].isna() | df['price'].isna())].copy()
    # Coerce numeric
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    # Drop any remaining with NaN after coercion
    df = df.dropna(subset=['quantity','price']).copy()
    # cast quantity to int where possible
    df['quantity'] = df['quantity'].astype(int)
    # Standardize dates (coerce errors to NaT)
    df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce').dt.strftime('%Y-%m-%d')
    after = len(df)
    return df, {'before_count': before, 'after_count': after}


# Load data

customers = pd.read_csv('../Preprocessed Datasets/customer_dataset.csv')
products = pd.read_csv('../Preprocessed Datasets/product_dataset.csv')
transactions = pd.read_csv('../Preprocessed Datasets/transaction_dataset.csv')

# Clean data
customers_clean, cust_report = clean_customers(customers)
products_clean, prod_report = clean_products(products)
transactions_clean, trans_report = clean_transactions(transactions)

print("Customer cleaning report:", cust_report)
print("Product cleaning report:", prod_report)
print("Transaction cleaning report:", trans_report)