# Databricks notebook source

# STEP 1: READ BRONZE ORDERS TABLE

orders_df = spark.table("workspace.bronze.orders")

print("BRONZE ORDERS DATA")
print("Total rows:", orders_df.count())

print("\nOriginal columns:")
orders_df.printSchema()

print("\nFirst 5 records:")
orders_df.show(5, truncate=False)


# COMMAND ----------


# STEP 2: CLEAN ORDERS DATA

from pyspark.sql.functions import col, trim, lower, to_timestamp

# Read the Bronze table
orders_silver = spark.table("workspace.bronze.orders")

# Remove unnecessary spaces from text columns
orders_silver = orders_silver.withColumn(
    "order_id", trim(col("order_id"))
).withColumn(
    "customer_id", trim(col("customer_id"))
).withColumn(
    "order_status", lower(trim(col("order_status")))
)

# Convert timestamp columns from text to timestamp type
timestamp_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for column_name in timestamp_columns:
    orders_silver = orders_silver.withColumn(
        column_name,
        to_timestamp(col(column_name))
    )

# Remove duplicate orders using the order ID
orders_silver = orders_silver.dropDuplicates(["order_id"])

print("ORDERS CLEANING COMPLETED!")
print("Cleaned row count:", orders_silver.count())

print("\nCleaned schema:")
orders_silver.printSchema()

print("\nCleaned sample records:")
orders_silver.show(5, truncate=False)


# COMMAND ----------


# STEP 3: SAVE CLEANED ORDERS TO SILVER

orders_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.orders")

print("SUCCESS: Silver orders table created!")

silver_orders = spark.table("workspace.silver.orders")

print("Silver orders row count:", silver_orders.count())


# COMMAND ----------


# STEP 4: READ BRONZE CUSTOMERS TABLE

customers_df = spark.table("workspace.bronze.customers")

print("Total customer records:", customers_df.count())

print("\nOriginal schema:")
customers_df.printSchema()

print("\nFirst 5 records:")
customers_df.show(5, truncate=False)


# COMMAND ----------


# STEP 5: CLEAN CUSTOMERS DATA

from pyspark.sql.functions import col, trim, upper

customers_silver = spark.table("workspace.bronze.customers")

# Remove unnecessary spaces
customers_silver = customers_silver.withColumn(
    "customer_id", trim(col("customer_id"))
).withColumn(
    "customer_unique_id", trim(col("customer_unique_id"))
).withColumn(
    "customer_city", trim(col("customer_city"))
).withColumn(
    "customer_state", upper(trim(col("customer_state")))
)

# Remove records without essential customer identifiers
customers_silver = customers_silver.filter(
    col("customer_id").isNotNull() &
    (col("customer_id") != "") &
    col("customer_unique_id").isNotNull() &
    (col("customer_unique_id") != "")
)

# Keep one record per customer_id
customers_silver = customers_silver.dropDuplicates(["customer_id"])

print("CUSTOMER CLEANING COMPLETED!")
print("Cleaned records:", customers_silver.count())

print("\nCleaned sample:")
customers_silver.show(5, truncate=False)


# COMMAND ----------


# STEP 6: SAVE CUSTOMERS TO SILVER

customers_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.customers")

print("SUCCESS: Silver customers table created!")

silver_customers = spark.table("workspace.silver.customers")

print("Silver customers row count:", silver_customers.count())


# COMMAND ----------


# VERIFY SILVER CUSTOMERS TABLE

saved_customers = spark.table("workspace.silver.customers")

print("Customers table saved successfully!")
print("Table location: workspace.silver.customers")
print("Total records:", saved_customers.count())

saved_customers.show(5, truncate=False)


# COMMAND ----------


# STEP 7: READ BRONZE ORDER ITEMS

items_df = spark.table("workspace.bronze.order_items")

print("Total order item records:", items_df.count())

print("\nOriginal schema:")
items_df.printSchema()

print("\nFirst 5 records:")
items_df.show(5, truncate=False)


# COMMAND ----------


# STEP 8: CLEAN ORDER ITEMS DATA

from pyspark.sql.functions import col, trim, to_timestamp
from pyspark.sql.types import DecimalType

# Read original data from Bronze
items_silver = spark.table("workspace.bronze.order_items")

# Clean text columns
text_columns = ["order_id", "product_id", "seller_id"]

for column_name in text_columns:
    items_silver = items_silver.withColumn(
        column_name,
        trim(col(column_name))
    )

# Convert data types
items_silver = (
    items_silver
    .withColumn("order_item_id", col("order_item_id").cast("integer"))
    .withColumn("price", col("price").cast(DecimalType(12, 2)))
    .withColumn("freight_value", col("freight_value").cast(DecimalType(12, 2)))
    .withColumn("shipping_limit_date", to_timestamp(col("shipping_limit_date")))
)

# Remove records without essential identifiers
items_silver = items_silver.filter(
    col("order_id").isNotNull() &
    (col("order_id") != "") &
    col("order_item_id").isNotNull() &
    col("product_id").isNotNull() &
    col("seller_id").isNotNull()
)

# Remove duplicate order items
items_silver = items_silver.dropDuplicates(
    ["order_id", "order_item_id"]
)

print("ORDER ITEMS CLEANING COMPLETED!")

print("Cleaned records:", items_silver.count())

print("\nCleaned schema:")
items_silver.printSchema()

print("\nFirst 5 cleaned records:")
items_silver.show(5, truncate=False)


# COMMAND ----------


# STEP 9: SAVE ORDER ITEMS TO SILVER

items_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.order_items")

print("SUCCESS: Silver order_items table created!")

saved_items = spark.table("workspace.silver.order_items")

print("Saved records:", saved_items.count())
print("Table: workspace.silver.order_items")


# COMMAND ----------


# STEP 10: VERIFY ORDER ITEMS TABLE

spark.table("workspace.silver.order_items").show(5, truncate=False)

print("Silver order_items table is available!")


# COMMAND ----------


# STEP 11: READ BRONZE ORDER PAYMENTS

payments_df = spark.table("workspace.bronze.order_payments")

print("Total payment records:", payments_df.count())

print("\nOriginal schema:")
payments_df.printSchema()

print("\nFirst 5 records:")
payments_df.show(5, truncate=False)


# COMMAND ----------


# STEP 12: CLEAN ORDER PAYMENTS

from pyspark.sql.functions import col, trim, lower
from pyspark.sql.types import DecimalType

payments_silver = spark.table("workspace.bronze.order_payments")

# Clean text columns
payments_silver = payments_silver.withColumn(
    "order_id", trim(col("order_id"))
).withColumn(
    "payment_type", lower(trim(col("payment_type")))
)

# Convert numeric columns
payments_silver = payments_silver.withColumn(
    "payment_sequential", col("payment_sequential").cast("integer")
).withColumn(
    "payment_installments", col("payment_installments").cast("integer")
).withColumn(
    "payment_value", col("payment_value").cast(DecimalType(12, 2))
)

# Remove records without essential identifiers
payments_silver = payments_silver.filter(
    col("order_id").isNotNull() &
    (col("order_id") != "") &
    col("payment_sequential").isNotNull()
)

# Remove duplicate payment records using their business key
payments_silver = payments_silver.dropDuplicates(
    ["order_id", "payment_sequential"]
)

print("PAYMENTS CLEANING COMPLETED!")
print("Cleaned records:", payments_silver.count())

print("\nCleaned schema:")
payments_silver.printSchema()

print("\nFirst 5 cleaned records:")
payments_silver.show(5, truncate=False)


# COMMAND ----------


# STEP 13: SAVE PAYMENTS TO SILVER

payments_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.order_payments")

print("SUCCESS: Silver order_payments table created!")

saved_payments = spark.table("workspace.silver.order_payments")

print("Saved payment records:", saved_payments.count())
print("Table: workspace.silver.order_payments")


# COMMAND ----------


# STEP 14: READ BRONZE ORDER REVIEWS

reviews_df = spark.table("workspace.bronze.order_reviews")

print("Total review records:", reviews_df.count())

print("\nOriginal schema:")
reviews_df.printSchema()

print("\nFirst 5 records:")
reviews_df.show(5, truncate=False)


# COMMAND ----------


# CHECK ORDER REVIEWS DATA QUALITY

from pyspark.sql.functions import col, count, countDistinct

reviews_df = spark.table("workspace.bronze.order_reviews")

print("TOTAL ROWS:", reviews_df.count())

print(
    "DISTINCT REVIEW IDs:",
    reviews_df.select("review_id").distinct().count()
)

print(
    "NULL REVIEW IDs:",
    reviews_df.filter(col("review_id").isNull()).count()
)

print(
    "NULL ORDER IDs:",
    reviews_df.filter(col("order_id").isNull()).count()
)

print(
    "INVALID REVIEW SCORES:",
    reviews_df.filter(
        ~col("review_score").cast("int").between(1, 5)
        | col("review_score").isNull()
    ).count()
)

print("\nDUPLICATE REVIEW IDs:")
reviews_df.groupBy("review_id") \
    .count() \
    .filter(col("count") > 1) \
    .show(10, truncate=False)


# COMMAND ----------


# INVESTIGATE INVALID REVIEW RECORDS

from pyspark.sql.functions import col, expr

reviews_df = spark.table("workspace.bronze.order_reviews")

# Safely convert review_score to integer
reviews_checked = reviews_df.withColumn(
    "score_as_integer",
    expr("try_cast(review_score AS INT)")
)

print("TOTAL RECORDS:", reviews_df.count())

print("\nINVALID OR MISSING REVIEW SCORES:")

reviews_checked.filter(
    col("score_as_integer").isNull()
).select(
    "review_id",
    "order_id",
    "review_score",
    "review_comment_title",
    "review_comment_message"
).show(20, truncate=False)

print("\nDUPLICATE REVIEW IDS:")

reviews_df.groupBy("review_id") \
    .count() \
    .filter(col("count") > 1) \
    .orderBy(col("count").desc()) \
    .show(20, truncate=False)

print("\nRECORDS WITH MISSING ORDER IDs:")

reviews_df.filter(
    col("order_id").isNull()
).show(10, truncate=False)


# COMMAND ----------


# VERIFY CORRECTED REVIEWS DATA

from pyspark.sql.functions import col, expr

reviews_df = spark.table("workspace.bronze.order_reviews")

reviews_checked = reviews_df.withColumn(
    "score_as_integer",
    expr("try_cast(review_score AS INT)")
)

print("TOTAL ROWS:", reviews_df.count())

print(
    "DISTINCT REVIEW IDs:",
    reviews_df.select("review_id").distinct().count()
)

print(
    "INVALID REVIEW SCORES:",
    reviews_checked.filter(
        col("score_as_integer").isNull()
    ).count()
)

print(
    "MISSING ORDER IDs:",
    reviews_df.filter(
        col("order_id").isNull()
    ).count()
)

print("\nSAMPLE RECORDS:")
reviews_df.show(10, truncate=False)


# COMMAND ----------


# INVESTIGATE DUPLICATE REVIEW IDs

from pyspark.sql.functions import col, count

reviews_df = spark.table("workspace.bronze.order_reviews")

duplicate_ids = (
    reviews_df
    .groupBy("review_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
    .orderBy(col("record_count").desc())
)

print("Number of duplicate review ID groups:", duplicate_ids.count())

print("\nDuplicate review IDs:")
duplicate_ids.show(15, truncate=False)

# Display the actual records for duplicate IDs
sample_duplicate = duplicate_ids.limit(1).select("review_id").collect()

if sample_duplicate:
    duplicate_review_id = sample_duplicate[0]["review_id"]

    print("\nActual records for one duplicate review ID:")
    reviews_df.filter(
        col("review_id") == duplicate_review_id
    ).show(truncate=False)


# COMMAND ----------


# CHECK WHETHER DUPLICATE REVIEW RECORDS ARE IDENTICAL

from pyspark.sql.functions import col, count

reviews_df = spark.table("workspace.bronze.order_reviews")

# Find review IDs that appear more than once
duplicate_ids = (
    reviews_df
    .groupBy("review_id")
    .count()
    .filter(col("count") > 1)
)

# Display records belonging to duplicate review IDs
duplicate_records = reviews_df.join(
    duplicate_ids.select("review_id"),
    on="review_id",
    how="inner"
)

print("TOTAL RECORDS WITH DUPLICATE REVIEW IDs:",
      duplicate_records.count())

print("\nSAMPLE DUPLICATE RECORDS:")
duplicate_records.orderBy("review_id").show(20, truncate=False)

# Check exact duplicate rows
print("\nEXACT DUPLICATE ROW COUNT:",
      reviews_df.count() - reviews_df.distinct().count())


# COMMAND ----------


# STEP 15: CLEAN ORDER REVIEWS DATA

from pyspark.sql.functions import col, trim, to_timestamp
from pyspark.sql.types import IntegerType

# Read corrected Bronze reviews
reviews_silver = spark.table("workspace.bronze.order_reviews")

# Clean text columns
text_columns = [
    "review_id",
    "order_id",
    "review_comment_title",
    "review_comment_message"
]

for column_name in text_columns:
    reviews_silver = reviews_silver.withColumn(
        column_name,
        trim(col(column_name))
    )

# Convert review score safely
reviews_silver = reviews_silver.withColumn(
    "review_score",
    col("review_score").cast(IntegerType())
)

# Convert date columns
reviews_silver = reviews_silver.withColumn(
    "review_creation_date",
    to_timestamp(col("review_creation_date"))
).withColumn(
    "review_answer_timestamp",
    to_timestamp(col("review_answer_timestamp"))
)

# Keep records with valid identifiers and review scores
reviews_silver = reviews_silver.filter(
    col("review_id").isNotNull() &
    (col("review_id") != "") &
    col("order_id").isNotNull() &
    (col("order_id") != "") &
    col("review_score").between(1, 5)
)

# Remove only completely identical records
reviews_silver = reviews_silver.dropDuplicates()

print("REVIEWS CLEANING COMPLETED!")
print("Cleaned records:", reviews_silver.count())

print("\nCleaned schema:")
reviews_silver.printSchema()

print("\nSample cleaned records:")
reviews_silver.show(10, truncate=False)


# COMMAND ----------


# STEP 16: SAVE REVIEWS TO SILVER

reviews_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.order_reviews")

print("SUCCESS: Silver reviews table created!")

saved_reviews = spark.table("workspace.silver.order_reviews")

print("Saved review records:", saved_reviews.count())
print("Table: workspace.silver.order_reviews")


# COMMAND ----------


# VERIFY SILVER REVIEWS TABLE

saved_reviews = spark.table("workspace.silver.order_reviews")

print("Total Silver reviews:", saved_reviews.count())
print("Distinct review IDs:",
      saved_reviews.select("review_id").distinct().count())

print("\nReview score distribution:")
saved_reviews.groupBy("review_score").count().orderBy(
    "review_score"
).show()


# COMMAND ----------


# VERIFY SILVER REVIEWS TABLE

saved_reviews = spark.table("workspace.silver.order_reviews")

print("Total Silver reviews:", saved_reviews.count())
print("Distinct review IDs:",
      saved_reviews.select("review_id").distinct().count())

print("\nReview score distribution:")
saved_reviews.groupBy("review_score").count().orderBy(
    "review_score"
).show()


# COMMAND ----------


# STEP 17: READ BRONZE PRODUCTS

products_df = spark.table("workspace.bronze.products")

print("TOTAL PRODUCTS:", products_df.count())

print("\nORIGINAL SCHEMA:")
products_df.printSchema()

print("\nFIRST 10 RECORDS:")
products_df.show(10, truncate=False)


# COMMAND ----------


# STEP 18: CHECK MISSING PRODUCT VALUES

from pyspark.sql.functions import col, sum

products_df = spark.table("workspace.bronze.products")

for column_name in products_df.columns:
    missing_count = products_df.filter(
        col(column_name).isNull()
    ).count()

    print(column_name, ":", missing_count, "missing values")


# COMMAND ----------

from pyspark.sql.functions import col, trim
from pyspark.sql.types import IntegerType

# STEP 19: READ BRONZE PRODUCTS
products_silver = spark.table("workspace.bronze.products")

# STEP 20: CLEAN TEXT COLUMNS
products_silver = products_silver.withColumn(
    "product_id", trim(col("product_id"))
).withColumn(
    "product_category_name", trim(col("product_category_name"))
)

# STEP 21: CONVERT NUMERIC COLUMNS
numeric_columns = [
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
]

for column_name in numeric_columns:
    products_silver = products_silver.withColumn(
        column_name, col(column_name).cast(IntegerType())
    )

# STEP 22: REMOVE RECORDS WITHOUT PRODUCT ID
products_silver = products_silver.filter(
    col("product_id").isNotNull() & (col("product_id") != "")
)

# STEP 23: REMOVE DUPLICATE PRODUCT IDs
products_silver = products_silver.dropDuplicates(["product_id"])

# STEP 24: SAVE SILVER TABLE
products_silver.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.silver.products"
)

print("Products Silver table created successfully!")
print("Total Silver Products:", products_silver.count())

products_silver.show(10, truncate=False)

# COMMAND ----------

# STEP 25: READ BRONZE SELLERS

sellers_df = spark.table("workspace.bronze.sellers")

print("TOTAL SELLERS:", sellers_df.count())

print("\nSELLERS SCHEMA:")
sellers_df.printSchema()

print("\nFIRST 10 RECORDS:")
sellers_df.show(10, truncate=False)

# COMMAND ----------

# STEP 26: CHECK MISSING SELLER VALUES

from pyspark.sql.functions import col

sellers_df = spark.table("workspace.bronze.sellers")

for column_name in sellers_df.columns:
    missing_count = sellers_df.filter(
        col(column_name).isNull()
    ).count()

    print(column_name, ":", missing_count, "missing values")

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper

# STEP 27: READ BRONZE SELLERS
sellers_silver = spark.table("workspace.bronze.sellers")

# STEP 28: CLEAN TEXT COLUMNS
sellers_silver = (
    sellers_silver
    .withColumn("seller_id", trim(col("seller_id")))
    .withColumn("seller_zip_code_prefix", trim(col("seller_zip_code_prefix")))
    .withColumn("seller_city", trim(col("seller_city")))
    .withColumn("seller_state", upper(trim(col("seller_state"))))
)

# STEP 29: REMOVE INVALID SELLER IDs
sellers_silver = sellers_silver.filter(
    col("seller_id").isNotNull() & (col("seller_id") != "")
)

# STEP 30: REMOVE DUPLICATE SELLER IDs
sellers_silver = sellers_silver.dropDuplicates(["seller_id"])

# STEP 31: SAVE SILVER TABLE
sellers_silver.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.silver.sellers"
)

# STEP 32: VALIDATE
print("Sellers Silver table created successfully!")
print("Total Silver Sellers:", sellers_silver.count())

sellers_silver.show(10, truncate=False)

# COMMAND ----------

spark.sql("""
SELECT COUNT(*) AS total_sellers,
       COUNT(DISTINCT seller_id) AS unique_sellers
FROM workspace.silver.sellers
""").show()

# COMMAND ----------

# STEP 33: READ BRONZE GEOLOCATION

geo_df = spark.table("workspace.bronze.geolocation")

print("TOTAL GEOLOCATION RECORDS:", geo_df.count())

print("\nSCHEMA:")
geo_df.printSchema()

print("\nFIRST 10 RECORDS:")
geo_df.show(10, truncate=False)

# COMMAND ----------

# STEP 34: CHECK MISSING VALUES

from pyspark.sql.functions import col

geo_df = spark.table("workspace.bronze.geolocation")

for column_name in geo_df.columns:
    missing_count = geo_df.filter(
        col(column_name).isNull()
    ).count()

    print(column_name, ":", missing_count, "missing values")

# COMMAND ----------

# STEP 35: CHECK DUPLICATE GEOLOCATION RECORDS

total_records = geo_df.count()
unique_records = geo_df.dropDuplicates().count()

print("TOTAL RECORDS:", total_records)
print("UNIQUE RECORDS:", unique_records)
print("DUPLICATE RECORDS:", total_records - unique_records)

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper
from pyspark.sql.types import DoubleType

# STEP 36: READ BRONZE GEOLOCATION
geo_silver = spark.table("workspace.bronze.geolocation")

# STEP 37: CLEAN TEXT COLUMNS
geo_silver = (
    geo_silver
    .withColumn(
        "geolocation_zip_code_prefix",
        trim(col("geolocation_zip_code_prefix"))
    )
    .withColumn(
        "geolocation_city",
        trim(col("geolocation_city"))
    )
    .withColumn(
        "geolocation_state",
        upper(trim(col("geolocation_state")))
    )
)

# STEP 38: CONVERT COORDINATES
geo_silver = (
    geo_silver
    .withColumn("geolocation_lat", col("geolocation_lat").cast(DoubleType()))
    .withColumn("geolocation_lng", col("geolocation_lng").cast(DoubleType()))
)

# STEP 39: REMOVE ROWS WITH MISSING ZIP CODE
geo_silver = geo_silver.filter(
    col("geolocation_zip_code_prefix").isNotNull()
)

# STEP 40: REMOVE EXACT DUPLICATES
geo_silver = geo_silver.dropDuplicates()

# STEP 41: SAVE SILVER TABLE
geo_silver.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.silver.geolocation"
)

# STEP 42: VALIDATE
print("Geolocation Silver table created successfully!")
print("Total Silver Records:", geo_silver.count())

geo_silver.show(10, truncate=False)

# COMMAND ----------

spark.sql("""
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT geolocation_zip_code_prefix) AS unique_zip_codes
FROM workspace.silver.geolocation
""").show()

# COMMAND ----------

# STEP 43: READ BRONZE CATEGORY TRANSLATION

category_df = spark.table("workspace.bronze.category_translation")

print("TOTAL CATEGORY RECORDS:", category_df.count())

print("\nSCHEMA:")
category_df.printSchema()

print("\nALL CATEGORY RECORDS:")
category_df.show(100, truncate=False)

# COMMAND ----------

# STEP 44: CHECK MISSING VALUES

from pyspark.sql.functions import col

category_df = spark.table("workspace.bronze.category_translation")

for column_name in category_df.columns:
    missing_count = category_df.filter(
        col(column_name).isNull()
    ).count()

    print(column_name, ":", missing_count, "missing values")

# COMMAND ----------

# STEP 45: CHECK DUPLICATE CATEGORY RECORDS

total_records = category_df.count()
unique_records = category_df.dropDuplicates().count()

print("TOTAL RECORDS:", total_records)
print("UNIQUE RECORDS:", unique_records)
print("DUPLICATE RECORDS:", total_records - unique_records)

# COMMAND ----------

from pyspark.sql.functions import col, trim

# STEP 46: READ BRONZE CATEGORY TRANSLATION
category_silver = spark.table(
    "workspace.bronze.category_translation"
)

# STEP 47: CLEAN CATEGORY NAMES
category_silver = (
    category_silver
    .withColumn(
        "product_category_name",
        trim(col("product_category_name"))
    )
    .withColumn(
        "product_category_name_english",
        trim(col("product_category_name_english"))
    )
)

# STEP 48: REMOVE INVALID CATEGORY RECORDS
category_silver = category_silver.filter(
    col("product_category_name").isNotNull() &
    (col("product_category_name") != "") &
    col("product_category_name_english").isNotNull() &
    (col("product_category_name_english") != "")
)

# STEP 49: REMOVE DUPLICATES
category_silver = category_silver.dropDuplicates(
    ["product_category_name"]
)

# STEP 50: SAVE SILVER TABLE
category_silver.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.silver.category_translation"
)

# STEP 51: VALIDATE
print("Category Translation Silver table created successfully!")
print("Total Silver Categories:", category_silver.count())

category_silver.show(100, truncate=False)

# COMMAND ----------

spark.sql("""
SELECT
    COUNT(*) AS total_categories,
    COUNT(DISTINCT product_category_name) AS unique_portuguese_categories
FROM workspace.silver.category_translation
""").show()

# COMMAND ----------

# STEP 52: READ BRONZE WEATHER

weather_df = spark.table("workspace.bronze.weather")

print("TOTAL WEATHER RECORDS:", weather_df.count())

print("\nSCHEMA:")
weather_df.printSchema()

print("\nFIRST 10 RECORDS:")
weather_df.show(10, truncate=False)

# COMMAND ----------

# STEP 53: CHECK MISSING WEATHER VALUES

from pyspark.sql.functions import col

weather_df = spark.table("workspace.bronze.weather")

for column_name in weather_df.columns:
    missing_count = weather_df.filter(
        col(column_name).isNull()
    ).count()

    print(column_name, ":", missing_count, "missing values")

# COMMAND ----------

# STEP 54: CHECK DUPLICATE WEATHER RECORDS

total_records = weather_df.count()
unique_records = weather_df.dropDuplicates().count()

print("TOTAL RECORDS:", total_records)
print("UNIQUE RECORDS:", unique_records)
print("DUPLICATE RECORDS:", total_records - unique_records)

# COMMAND ----------

from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType

# STEP 55: READ BRONZE WEATHER
weather_silver = spark.table("workspace.bronze.weather")

# STEP 56: CONVERT DATE COLUMN
weather_silver = weather_silver.withColumn(
    "date", to_date(col("date"))
)

# STEP 57: CONVERT WEATHER MEASUREMENTS
weather_silver = (
    weather_silver
    .withColumn(
        "temperature_2m_mean",
        col("temperature_2m_mean").cast(DoubleType())
    )
    .withColumn(
        "precipitation_sum",
        col("precipitation_sum").cast(DoubleType())
    )
)

# STEP 58: REMOVE ROWS WITH INVALID DATES
weather_silver = weather_silver.filter(
    col("date").isNotNull()
)

# STEP 59: REMOVE EXACT DUPLICATES
weather_silver = weather_silver.dropDuplicates()

# STEP 60: SAVE SILVER WEATHER TABLE
weather_silver.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.silver.weather"
)

# STEP 61: VALIDATE
print("Weather Silver table created successfully!")
print("Total Silver Weather Records:", weather_silver.count())

weather_silver.printSchema()
weather_silver.show(10, truncate=False)

# COMMAND ----------

spark.sql("""
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT date) AS unique_dates,
    MIN(date) AS first_date,
    MAX(date) AS last_date
FROM workspace.silver.weather
""").show()

# COMMAND ----------

# STEP 62: READ BRONZE BRAZILIAN HOLIDAYS

holidays_df = spark.table("workspace.bronze.brazil_holidays")

print("TOTAL HOLIDAY RECORDS:", holidays_df.count())

print("\nSCHEMA:")
holidays_df.printSchema()

print("\nALL HOLIDAY RECORDS:")
holidays_df.show(50, truncate=False)

# COMMAND ----------

# STEP 63: CHECK MISSING HOLIDAY VALUES

from pyspark.sql.functions import col

holidays_df = spark.table("workspace.bronze.brazil_holidays")

for column_name in holidays_df.columns:
    missing_count = holidays_df.filter(
        col(column_name).isNull()
    ).count()

    print(column_name, ":", missing_count, "missing values")

# COMMAND ----------

# STEP 64: CHECK DUPLICATE HOLIDAY RECORDS

total_records = holidays_df.count()
unique_records = holidays_df.dropDuplicates().count()

print("TOTAL RECORDS:", total_records)
print("UNIQUE RECORDS:", unique_records)
print("DUPLICATE RECORDS:", total_records - unique_records)

# COMMAND ----------

from pyspark.sql.functions import col, trim, to_date

# STEP 65: READ BRONZE HOLIDAYS
holidays_silver = spark.table(
    "workspace.bronze.brazil_holidays"
)

# STEP 66: CLEAN TEXT COLUMNS
holidays_silver = (
    holidays_silver
    .withColumn("localName", trim(col("localName")))
    .withColumn("name", trim(col("name")))
    .withColumn("countryCode", trim(col("countryCode")))
)

# STEP 67: CONVERT DATE COLUMN
holidays_silver = holidays_silver.withColumn(
    "date", to_date(col("date"))
)

# STEP 68: CONVERT GLOBAL COLUMN TO BOOLEAN
holidays_silver = holidays_silver.withColumn(
    "global", col("global").cast("boolean")
)

# STEP 69: REMOVE INVALID RECORDS
holidays_silver = holidays_silver.filter(
    col("date").isNotNull() &
    col("name").isNotNull() &
    (col("name") != "")
)

# STEP 70: REMOVE EXACT DUPLICATES
holidays_silver = holidays_silver.dropDuplicates()

# STEP 71: SAVE SILVER TABLE
holidays_silver.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.silver.brazil_holidays"
)

# STEP 72: VALIDATE
print("Brazilian Holidays Silver table created successfully!")
print("Total Silver Holiday Records:", holidays_silver.count())

holidays_silver.printSchema()
holidays_silver.show(50, truncate=False)

# COMMAND ----------

spark.sql("""
SELECT
    COUNT(*) AS total_holidays,
    COUNT(DISTINCT date) AS unique_dates,
    MIN(date) AS first_date,
    MAX(date) AS last_date
FROM workspace.silver.brazil_holidays
""").show()

# COMMAND ----------

# FINAL SILVER TABLE VALIDATION

silver_tables = [
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

for table_name in silver_tables:
    full_table_name = f"workspace.silver.{table_name}"

    count = spark.table(full_table_name).count()

    print(table_name, ":", count, "records")

# COMMAND ----------

# FINAL SILVER DATA TYPE VALIDATION

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

    print("\n==========", table_name.upper(), "==========")

    for column_name, data_type in df.dtypes:
        print(column_name, ":", data_type)