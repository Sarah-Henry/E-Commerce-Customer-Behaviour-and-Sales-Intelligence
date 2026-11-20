import pandas as pd
import os

# Location of our raw dataset
folder = "data/raw"

# Expected dataset files
files = [
    "olist_orders_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "product_category_name_translation.csv"
]

print("====================================")
print("   OLIST DATASET VALIDATION REPORT")
print("====================================")

for file in files:

    path = os.path.join(folder, file)

    print("\n------------------------------------")
    print("FILE:", file)
    print("------------------------------------")

    if not os.path.exists(path):
        print("ERROR: File not found!")
        continue

    try:
        df = pd.read_csv(path)

        print("Status: Successfully loaded")
        print("Rows:", len(df))
        print("Columns:", len(df.columns))
        print("Column names:", df.columns.tolist())
        print("Duplicate rows:", df.duplicated().sum())
        print("Missing values:", df.isnull().sum().sum())

        print("\nFirst 5 records:")
        print(df.head())

    except Exception as e:
        print("Error reading file:", e)

print("\n====================================")
print("       VALIDATION COMPLETED")
print("====================================")
