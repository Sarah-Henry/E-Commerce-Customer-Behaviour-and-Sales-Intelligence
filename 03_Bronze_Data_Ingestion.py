# Databricks notebook source

# Location of our uploaded project files
volume_path = "/Volumes/workspace/default/ecommerce_raw"

# List all uploaded files
files = dbutils.fs.ls(volume_path)

print("FILES AVAILABLE IN OUR PROJECT VOLUME:")
print("---------------------------------------")

for file in files:
    print(file.name, "-", file.size, "bytes")

print("---------------------------------------")
print("Total files found:", len(files))


# COMMAND ----------


from pyspark.sql.functions import current_timestamp, col

# Step 1: Define the uploaded CSV file location
file_path = "/Volumes/workspace/default/ecommerce_raw/olist_orders_dataset.csv"

# Step 2: Read the original CSV file
orders_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .option("multiLine", "true")
    .option("mode", "PERMISSIVE")
    .load(file_path)
)

# Step 3: Add ingestion information
orders_df = (
    orders_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
)

# Step 4: Save as a Bronze Delta table
orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.bronze.orders")

# Step 5: Verify the table
bronze_orders = spark.table("workspace.bronze.orders")

print("BRONZE ORDERS TABLE CREATED SUCCESSFULLY!")
print("Total rows:", bronze_orders.count())

print("\nTable columns:")
bronze_orders.printSchema()

print("\nFirst 5 records:")
bronze_orders.show(5, truncate=False)


# COMMAND ----------


from pyspark.sql.functions import current_timestamp, col

# Project storage location
base_path = "/Volumes/workspace/default/ecommerce_raw"

# Remaining CSV files and their Bronze table names
datasets = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
    "weather": "weather_data.csv",
    "brazil_holidays": "brazil_holidays.csv"
}

# Load each CSV into its Bronze Delta table
for table_name, file_name in datasets.items():

    print("\nLoading:", file_name)

    file_path = f"{base_path}/{file_name}"

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .load(file_path)
    )

    df = (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"workspace.bronze.{table_name}")

    row_count = spark.table(
        f"workspace.bronze.{table_name}"
    ).count()

    print("SUCCESS:", table_name)
    print("Rows:", row_count)

print("\nALL REMAINING BRONZE TABLES LOADED!")


# COMMAND ----------


# STEP 1: BRONZE TABLE VALIDATION

bronze_tables = [
    "orders",
    "customers",
    "geolocation",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
    "category_translation",
    "weather",
    "brazil_holidays"
]

print("========== BRONZE LAYER VALIDATION ==========")

for table_name in bronze_tables:
    full_table_name = f"workspace.bronze.{table_name}"

    try:
        df = spark.table(full_table_name)
        print(f"{table_name}: {df.count()} rows | SUCCESS")
    except Exception as error:
        print(f"{table_name}: FAILED")
        print(error)

print("========== VALIDATION COMPLETED ==========")


# COMMAND ----------


# FIX: RELOAD ORDER REVIEWS WITH CSV QUOTE HANDLING

from pyspark.sql.functions import current_timestamp, col

file_path = "/Volumes/workspace/default/ecommerce_raw/olist_order_reviews_dataset.csv"

reviews_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("mode", "PERMISSIVE")
    .load(file_path)
)

reviews_df = (
    reviews_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
)

print("RELOADED REVIEWS ROWS:", reviews_df.count())

print("\nRELOADED REVIEWS SCHEMA:")
reviews_df.printSchema()

print("\nFIRST 10 RECORDS:")
reviews_df.show(10, truncate=False)

# Replace only the Bronze reviews table
reviews_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.bronze.order_reviews")

print("\nBRONZE REVIEWS TABLE RELOADED SUCCESSFULLY!")


# COMMAND ----------

# FINAL BRONZE TABLE VALIDATION

bronze_tables = [
    "orders",
    "customers",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
    "geolocation",
    "category_translation",
    "weather",
    "brazil_holidays"
]

for table_name in bronze_tables:
    full_table_name = f"workspace.bronze.{table_name}"

    count = spark.table(full_table_name).count()

    print(table_name, ":", count, "records")

# COMMAND ----------

# FINAL SCHEMA VALIDATION

tables = [
    "orders",
    "customers",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
    "geolocation",
    "category_translation",
    "weather",
    "brazil_holidays"
]

for table_name in tables:

    print("\n" + "=" * 60)
    print("TABLE:", table_name.upper())

    bronze_df = spark.table(f"workspace.bronze.{table_name}")
    silver_df = spark.table(f"workspace.silver.{table_name}")

    print("Bronze columns:", len(bronze_df.columns))
    print("Silver columns:", len(silver_df.columns))

    print("\nSilver schema:")
    silver_df.printSchema()

# COMMAND ----------

# FINAL SILVER SCHEMA SUMMARY

tables = [
    "orders",
    "customers",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
    "geolocation",
    "category_translation",
    "weather",
    "brazil_holidays"
]

for table_name in tables:
    df = spark.table(f"workspace.silver.{table_name}")

    print("\nTABLE:", table_name.upper())
    print("Columns:", len(df.columns))
    print("Column names:", df.columns)