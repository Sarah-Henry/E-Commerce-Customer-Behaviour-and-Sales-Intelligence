# Databricks notebook source
# STEP 2: CHECK SILVER TABLES FOR GOLD LAYER

tables = [
    "orders",
    "customers",
    "order_items",
    "order_payments",
    "order_reviews"
]

for table_name in tables:

    df = spark.table(f"workspace.silver.{table_name}")

    print("\nTABLE:", table_name.upper())
    print("TOTAL RECORDS:", df.count())
    print("COLUMNS:", df.columns)

# COMMAND ----------


from pyspark.sql.functions import (
    col, sum, count, avg, collect_set,
    concat_ws, sort_array, coalesce, lit
)

# 1. Read Silver tables
orders = spark.table("workspace.silver.orders")
customers = spark.table("workspace.silver.customers")
items = spark.table("workspace.silver.order_items")
payments = spark.table("workspace.silver.order_payments")
reviews = spark.table("workspace.silver.order_reviews")

# 2. Aggregate order items: one row per order
items_agg = (
    items.groupBy("order_id")
    .agg(
        count("order_item_id").alias("item_count"),
        sum("price").alias("total_product_revenue"),
        sum("freight_value").alias("total_freight_value")
    )
)

# 3. Aggregate payments: one row per order
payments_agg = (
    payments.groupBy("order_id")
    .agg(
        sum("payment_value").alias("total_payment_value"),
        count("payment_sequential").alias("payment_count"),
        concat_ws(", ", sort_array(collect_set("payment_type"))).alias("payment_types")
    )
)

# 4. Aggregate reviews: one row per order
reviews_agg = (
    reviews.groupBy("order_id")
    .agg(
        avg("review_score").alias("average_review_score"),
        count("review_id").alias("review_count")
    )
)

# 5. Join all aggregated tables with orders and customers
gold_orders = (
    orders.alias("o")
    .join(customers.alias("c"), col("o.customer_id") == col("c.customer_id"), "left")
    .join(items_agg.alias("i"), col("o.order_id") == col("i.order_id"), "left")
    .join(payments_agg.alias("p"), col("o.order_id") == col("p.order_id"), "left")
    .join(reviews_agg.alias("r"), col("o.order_id") == col("r.order_id"), "left")
    .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("c.customer_unique_id"),
        col("c.customer_city"),
        col("c.customer_state"),
        col("o.order_status"),
        col("o.order_purchase_timestamp"),
        col("o.order_approved_at"),
        col("o.order_delivered_customer_date"),
        col("o.order_estimated_delivery_date"),
        coalesce(col("i.item_count"), lit(0)).alias("item_count"),
        coalesce(col("i.total_product_revenue"), lit(0)).alias("total_product_revenue"),
        coalesce(col("i.total_freight_value"), lit(0)).alias("total_freight_value"),
        coalesce(col("p.total_payment_value"), lit(0)).alias("total_payment_value"),
        coalesce(col("p.payment_count"), lit(0)).alias("payment_count"),
        col("p.payment_types"),
        col("r.average_review_score"),
        coalesce(col("r.review_count"), lit(0)).alias("review_count")
    )
)

# 6. Save as a Gold Delta table
gold_orders.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.gold.gold_orders"
)

print("gold_orders table created successfully!")


# COMMAND ----------


gold_df = spark.table("workspace.gold.gold_orders")

print("TOTAL ROWS:", gold_df.count())
print("DISTINCT ORDERS:", gold_df.select("order_id").distinct().count())

print("\nGOLD TABLE PREVIEW:")
gold_df.show(10, truncate=False)

print("\nGOLD TABLE SCHEMA:")
gold_df.printSchema()


# COMMAND ----------


from pyspark.sql.functions import (
    col,
    to_date,
    count,
    sum,
    avg,
    when,
    max as spark_max
)

# 1. Read Gold Orders and Silver external data
orders = spark.table("workspace.gold.gold_orders")
weather = spark.table("workspace.silver.weather")
holidays = spark.table("workspace.silver.brazil_holidays")

# 2. Create daily sales summary
daily_sales = (
    orders
    .withColumn("order_date", to_date(col("order_purchase_timestamp")))
    .groupBy("order_date")
    .agg(
        count("order_id").alias("total_orders"),

        sum("total_product_revenue").alias("total_product_revenue"),

        sum("total_freight_value").alias("total_freight_value"),

        sum("total_payment_value").alias("total_payment_value"),

        avg("average_review_score").alias("average_review_score"),

        sum(
            when(col("order_status") == "delivered", 1).otherwise(0)
        ).alias("delivered_orders"),

        sum(
            when(col("order_status") == "canceled", 1).otherwise(0)
        ).alias("cancelled_orders")
    )
)

# 3. Join São Paulo weather data
daily_sales = daily_sales.join(
    weather.select(
        col("date").alias("weather_date"),
        col("temperature_2m_mean").alias("sp_temperature_mean"),
        col("precipitation_sum").alias("sp_precipitation_sum")
    ),
    daily_sales.order_date == col("weather_date"),
    "left"
).drop("weather_date")

# 4. Join Brazilian public holidays
daily_sales = daily_sales.join(
    holidays.select(
        col("date").alias("holiday_date"),
        col("localName").alias("holiday_name")
    ),
    daily_sales.order_date == col("holiday_date"),
    "left"
).drop("holiday_date")

# 5. Add a holiday indicator
daily_sales = daily_sales.withColumn(
    "is_public_holiday",
    when(col("holiday_name").isNotNull(), True).otherwise(False)
)

# 6. Save as Gold Delta table
daily_sales.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.gold.gold_daily_sales"
)

print("Gold daily sales table created successfully!")

# 7. Display results
print("TOTAL ROWS:", daily_sales.count())

daily_sales.orderBy("order_date").show(10, truncate=False)


# COMMAND ----------


from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum,
    avg,
    coalesce
)

# 1. Read Silver tables
items = spark.table("workspace.silver.order_items")
products = spark.table("workspace.silver.products")
categories = spark.table("workspace.silver.category_translation")
orders = spark.table("workspace.silver.orders")

# 2. Join order items with product details and order status
product_data = (
    items
    .join(products, on="product_id", how="left")
    .join(categories, on="product_category_name", how="left")
    .join(
        orders.select("order_id", "order_status"),
        on="order_id",
        how="left"
    )
)

# 3. Group data product-wise
product_performance = (
    product_data
    .groupBy(
        "product_id",
        "product_category_name",
        "product_category_name_english"
    )
    .agg(
        count("order_item_id").alias("units_sold"),

        countDistinct("order_id").alias("total_orders"),

        sum("price").alias("total_product_revenue"),

        sum("freight_value").alias("total_freight_value"),

        avg("price").alias("average_selling_price"),

        sum(
            when(col("order_status") == "delivered", 1).otherwise(0)
        ).alias("delivered_units"),

        sum(
            when(
                col("order_status") == "delivered",
                col("price")
            ).otherwise(0)
        ).alias("delivered_product_revenue")
    )
)

# 4. Fill missing English category names
product_performance = product_performance.withColumn(
    "product_category_name_english",
    coalesce(
        col("product_category_name_english"),
        col("product_category_name")
    )
)

# 5. Save Gold table
product_performance.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.gold.gold_product_performance"
)

print("Gold product performance table created successfully!")

# 6. Validate records
print("TOTAL PRODUCTS:", product_performance.count())

print("TOTAL UNITS SOLD:", product_performance.agg(
    sum("units_sold")
).collect()[0][0])

# 7. Display top 10 products by revenue
print("TOP 10 PRODUCTS BY REVENUE:")

product_performance.orderBy(
    col("total_product_revenue").desc()
).show(10, truncate=False)


# COMMAND ----------


from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    min,
    max,
    when,
    round
)

# 1. Read Gold Orders table
orders = spark.table("workspace.gold.gold_orders")

# 2. Group by unique customer
customer_analytics = (
    orders
    .groupBy(
        "customer_unique_id",
        "customer_city",
        "customer_state"
    )
    .agg(
        count("order_id").alias("total_orders"),

        sum("total_product_revenue").alias("total_product_spend"),

        sum("total_freight_value").alias("total_freight_spend"),

        sum("total_payment_value").alias("total_payment_spend"),

        avg("total_product_revenue").alias("average_order_value"),

        avg("average_review_score").alias("average_review_score"),

        min("order_purchase_timestamp").alias("first_purchase_date"),

        max("order_purchase_timestamp").alias("last_purchase_date")
    )
)

# 3. Identify repeat customers
customer_analytics = customer_analytics.withColumn(
    "is_repeat_customer",
    when(col("total_orders") > 1, True).otherwise(False)
)

# 4. Save Gold table
customer_analytics.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.gold.gold_customer_analytics"
)

print("Gold customer analytics table created successfully!")

# 5. Validate
print("TOTAL UNIQUE CUSTOMERS:", customer_analytics.count())

print(
    "REPEAT CUSTOMERS:",
    customer_analytics.filter(
        col("is_repeat_customer") == True
    ).count()
)

# 6. Display top 10 customers by spending
print("TOP 10 CUSTOMERS BY PRODUCT SPEND:")

customer_analytics.orderBy(
    col("total_product_spend").desc()
).show(10, truncate=False)


# COMMAND ----------

from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    round,
    min,
    max
)

# 1. Read Silver payment data
payments = spark.table("workspace.silver.order_payments")

# 2. Group by payment method
payment_analytics = (
    payments
    .groupBy("payment_type")
    .agg(
        count("*").alias("total_transactions"),

        countDistinct("order_id").alias("total_orders"),

        sum("payment_value").alias("total_payment_value"),

        avg("payment_value").alias("average_payment_value"),

        avg("payment_installments").alias("average_installments"),

        min("payment_value").alias("minimum_payment"),

        max("payment_value").alias("maximum_payment")
    )
)

# 3. Save Gold table
payment_analytics.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.gold.gold_payment_analytics"
)

print("Gold payment analytics table created successfully!")

# 4. Validate total transactions
print("TOTAL TRANSACTIONS:", payments.count())

print("TOTAL PAYMENT METHODS:", payment_analytics.count())

# 5. Display payment method analysis
payment_analytics.orderBy(
    col("total_payment_value").desc()
).show(truncate=False)


# COMMAND ----------


from pyspark.sql.functions import (
    col,
    count,
    avg,
    sum,
    when,
    datediff,
    round
)

# 1. Read Silver orders and customers
orders = spark.table("workspace.silver.orders")
customers = spark.table("workspace.silver.customers")

# 2. Join orders with customer state
delivery_data = orders.join(
    customers.select("customer_id", "customer_state"),
    on="customer_id",
    how="left"
)

# 3. Calculate delivery days and delay days
delivery_data = (
    delivery_data
    .withColumn(
        "actual_delivery_days",
        datediff(
            col("order_delivered_customer_date"),
            col("order_purchase_timestamp")
        )
    )
    .withColumn(
        "delivery_delay_days",
        datediff(
            col("order_delivered_customer_date"),
            col("order_estimated_delivery_date")
        )
    )
)

# 4. Keep only delivered orders with delivery dates
delivered_data = delivery_data.filter(
    (col("order_status") == "delivered") &
    col("order_delivered_customer_date").isNotNull() &
    col("order_estimated_delivery_date").isNotNull()
)

# 5. Group by customer state
delivery_analytics = (
    delivered_data
    .groupBy("customer_state")
    .agg(
        count("order_id").alias("total_delivered_orders"),

        round(
            avg("actual_delivery_days"), 2
        ).alias("average_delivery_days"),

        sum(
            when(col("delivery_delay_days") > 0, 1).otherwise(0)
        ).alias("late_deliveries"),

        sum(
            when(col("delivery_delay_days") <= 0, 1).otherwise(0)
        ).alias("on_time_or_early_deliveries"),

        round(
            avg("delivery_delay_days"), 2
        ).alias("average_delivery_delay_days")
    )
)

# 6. Save Gold table
delivery_analytics.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.gold.gold_delivery_analytics"
)

print("Gold delivery analytics table created successfully!")

# 7. Validate
print("TOTAL DELIVERED ORDERS ANALYSED:", delivered_data.count())

print("TOTAL STATES:", delivery_analytics.count())

# 8. Display results
delivery_analytics.orderBy(
    col("total_delivered_orders").desc()
).show(truncate=False)


# COMMAND ----------


from pyspark.sql.functions import (
    col,
    count,
    avg,
    sum,
    round,
    when
)

# 1. Read Silver tables
reviews = spark.table("workspace.silver.order_reviews")
orders = spark.table("workspace.silver.orders")
customers = spark.table("workspace.silver.customers")

# 2. Join reviews with orders and customers
review_data = (
    reviews
    .join(
        orders.select("order_id", "customer_id"),
        on="order_id",
        how="left"
    )
    .join(
        customers.select("customer_id", "customer_state"),
        on="customer_id",
        how="left"
    )
)

# 3. Group by customer state
review_analytics = (
    review_data
    .groupBy("customer_state")
    .agg(
        count("review_id").alias("total_reviews"),

        round(
            avg("review_score"), 2
        ).alias("average_review_score"),

        sum(
            when(col("review_score") == 1, 1).otherwise(0)
        ).alias("one_star_reviews"),

        sum(
            when(col("review_score") == 2, 1).otherwise(0)
        ).alias("two_star_reviews"),

        sum(
            when(col("review_score") == 3, 1).otherwise(0)
        ).alias("three_star_reviews"),

        sum(
            when(col("review_score") == 4, 1).otherwise(0)
        ).alias("four_star_reviews"),

        sum(
            when(col("review_score") == 5, 1).otherwise(0)
        ).alias("five_star_reviews")
    )
)

# 4. Save Gold table
review_analytics.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.gold.gold_review_analytics"
)

print("Gold review analytics table created successfully!")

# 5. Validate
print("TOTAL REVIEW RECORDS:", review_data.count())

print("TOTAL STATES:", review_analytics.count())

# 6. Display results
review_analytics.orderBy(
    col("total_reviews").desc()
).show(truncate=False)


# COMMAND ----------


# ==========================================
# FINAL VALIDATION OF ALL 7 GOLD TABLES
# ==========================================

gold_orders = spark.table("workspace.gold.gold_orders")
gold_daily = spark.table("workspace.gold.gold_daily_sales")
gold_products = spark.table("workspace.gold.gold_product_performance")
gold_customers = spark.table("workspace.gold.gold_customer_analytics")
gold_payments = spark.table("workspace.gold.gold_payment_analytics")
gold_delivery = spark.table("workspace.gold.gold_delivery_analytics")
gold_reviews = spark.table("workspace.gold.gold_review_analytics")

silver_orders = spark.table("workspace.silver.orders")
silver_items = spark.table("workspace.silver.order_items")
silver_payments = spark.table("workspace.silver.order_payments")
silver_reviews = spark.table("workspace.silver.order_reviews")

print("========== GOLD TABLE ROW COUNTS ==========")

gold_tables = [
    ("gold_orders", gold_orders),
    ("gold_daily_sales", gold_daily),
    ("gold_product_performance", gold_products),
    ("gold_customer_analytics", gold_customers),
    ("gold_payment_analytics", gold_payments),
    ("gold_delivery_analytics", gold_delivery),
    ("gold_review_analytics", gold_reviews)
]

for name, df in gold_tables:
    print(name, ":", df.count(), "records")

print("\n========== VALIDATION CHECKS ==========")

# 1. Order count
print(
    "Orders match:",
    gold_orders.count() == silver_orders.count()
)

# 2. Unique order IDs
print(
    "No duplicate orders:",
    gold_orders.select("order_id").distinct().count()
    == gold_orders.count()
)

# 3. Daily sales order count
print(
    "Daily orders match:",
    gold_daily.agg({"total_orders": "sum"}).first()[0]
    == gold_orders.count()
)

# 4. Product units match order items
product_units = gold_products.agg(
    {"units_sold": "sum"}
).first()[0]

print(
    "Product units match:",
    product_units == silver_items.count()
)

# 5. Product revenue matches order revenue
product_revenue = gold_products.agg(
    {"total_product_revenue": "sum"}
).first()[0]

order_revenue = gold_orders.agg(
    {"total_product_revenue": "sum"}
).first()[0]

print(
    "Product revenue matches:",
    product_revenue == order_revenue
)

# 6. Payment transactions match
payment_transactions = gold_payments.agg(
    {"total_transactions": "sum"}
).first()[0]

print(
    "Payment transactions match:",
    payment_transactions == silver_payments.count()
)

# 7. Review records match
review_records = gold_reviews.agg(
    {"total_reviews": "sum"}
).first()[0]

print(
    "Review records match:",
    review_records == silver_reviews.count()
)

# 8. Customer table has unique customer IDs
print(
    "No duplicate unique customers:",
    gold_customers.select("customer_unique_id").distinct().count()
    == gold_customers.count()
)

# 9. Delivery records match eligible delivered orders
expected_delivered = silver_orders.filter(
    (col("order_status") == "delivered") &
    col("order_delivered_customer_date").isNotNull() &
    col("order_estimated_delivery_date").isNotNull()
).count()

actual_delivered = gold_delivery.agg(
    {"total_delivered_orders": "sum"}
).first()[0]

print(
    "Delivery records match:",
    expected_delivered == actual_delivered
)

print("\n========== VALIDATION COMPLETED ==========")


# COMMAND ----------


from pyspark.sql.functions import (
    col, count, sum, avg, min, max, when,
    row_number
)
from pyspark.sql.window import Window

# 1. Read Gold Orders
orders = spark.table("workspace.gold.gold_orders")

# 2. Aggregate all orders by unique customer ID only
customer_summary = (
    orders
    .groupBy("customer_unique_id")
    .agg(
        count("order_id").alias("total_orders"),
        sum("total_product_revenue").alias("total_product_spend"),
        sum("total_freight_value").alias("total_freight_spend"),
        sum("total_payment_value").alias("total_payment_spend"),
        avg("total_product_revenue").alias("average_order_value"),
        avg("average_review_score").alias("average_review_score"),
        min("order_purchase_timestamp").alias("first_purchase_date"),
        max("order_purchase_timestamp").alias("last_purchase_date")
    )
)

# 3. Find each customer's latest order location
window_spec = Window.partitionBy(
    "customer_unique_id"
).orderBy(
    col("order_purchase_timestamp").desc_nulls_last(),
    col("order_id").desc()
)

latest_location = (
    orders
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .select(
        "customer_unique_id",
        "customer_city",
        "customer_state"
    )
)

# 4. Combine customer summary with latest location
customer_analytics = (
    customer_summary
    .join(latest_location, on="customer_unique_id", how="left")
    .withColumn(
        "is_repeat_customer",
        when(col("total_orders") > 1, True).otherwise(False)
    )
)

# 5. Overwrite the existing Gold table
customer_analytics.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.gold.gold_customer_analytics"
)

print("Customer analytics table corrected successfully!")

# 6. Validate customer uniqueness
print("TOTAL CUSTOMER ROWS:", customer_analytics.count())

print(
    "DISTINCT CUSTOMER IDs:",
    customer_analytics.select("customer_unique_id").distinct().count()
)

print(
    "REPEAT CUSTOMERS:",
    customer_analytics.filter(
        col("is_repeat_customer") == True
    ).count()
)

print(
    "NO DUPLICATE CUSTOMERS:",
    customer_analytics.select("customer_unique_id").distinct().count()
    == customer_analytics.count()
)


# COMMAND ----------


# ==========================================
# FINAL VALIDATION OF ALL 7 GOLD TABLES
# ==========================================

gold_orders = spark.table("workspace.gold.gold_orders")
gold_daily = spark.table("workspace.gold.gold_daily_sales")
gold_products = spark.table("workspace.gold.gold_product_performance")
gold_customers = spark.table("workspace.gold.gold_customer_analytics")
gold_payments = spark.table("workspace.gold.gold_payment_analytics")
gold_delivery = spark.table("workspace.gold.gold_delivery_analytics")
gold_reviews = spark.table("workspace.gold.gold_review_analytics")

silver_orders = spark.table("workspace.silver.orders")
silver_items = spark.table("workspace.silver.order_items")
silver_payments = spark.table("workspace.silver.order_payments")
silver_reviews = spark.table("workspace.silver.order_reviews")

print("========== GOLD TABLE ROW COUNTS ==========")

gold_tables = [
    ("gold_orders", gold_orders),
    ("gold_daily_sales", gold_daily),
    ("gold_product_performance", gold_products),
    ("gold_customer_analytics", gold_customers),
    ("gold_payment_analytics", gold_payments),
    ("gold_delivery_analytics", gold_delivery),
    ("gold_review_analytics", gold_reviews)
]

for name, df in gold_tables:
    print(name, ":", df.count(), "records")

print("\n========== VALIDATION CHECKS ==========")

# 1. Order count
print(
    "Orders match:",
    gold_orders.count() == silver_orders.count()
)

# 2. Unique order IDs
print(
    "No duplicate orders:",
    gold_orders.select("order_id").distinct().count()
    == gold_orders.count()
)

# 3. Daily sales order count
print(
    "Daily orders match:",
    gold_daily.agg({"total_orders": "sum"}).first()[0]
    == gold_orders.count()
)

# 4. Product units match order items
product_units = gold_products.agg(
    {"units_sold": "sum"}
).first()[0]

print(
    "Product units match:",
    product_units == silver_items.count()
)

# 5. Product revenue matches order revenue
product_revenue = gold_products.agg(
    {"total_product_revenue": "sum"}
).first()[0]

order_revenue = gold_orders.agg(
    {"total_product_revenue": "sum"}
).first()[0]

print(
    "Product revenue matches:",
    product_revenue == order_revenue
)

# 6. Payment transactions match
payment_transactions = gold_payments.agg(
    {"total_transactions": "sum"}
).first()[0]

print(
    "Payment transactions match:",
    payment_transactions == silver_payments.count()
)

# 7. Review records match
review_records = gold_reviews.agg(
    {"total_reviews": "sum"}
).first()[0]

print(
    "Review records match:",
    review_records == silver_reviews.count()
)

# 8. Customer table has unique customer IDs
print(
    "No duplicate unique customers:",
    gold_customers.select("customer_unique_id").distinct().count()
    == gold_customers.count()
)

# 9. Delivery records match eligible delivered orders
expected_delivered = silver_orders.filter(
    (col("order_status") == "delivered") &
    col("order_delivered_customer_date").isNotNull() &
    col("order_estimated_delivery_date").isNotNull()
).count()

actual_delivered = gold_delivery.agg(
    {"total_delivered_orders": "sum"}
).first()[0]

print(
    "Delivery records match:",
    expected_delivered == actual_delivered
)

print("\n========== VALIDATION COMPLETED ==========")


# COMMAND ----------


# EXPORT GOLD TABLE 1: ORDERS

df = spark.table("workspace.gold.gold_orders")

print("Total records:", df.count())
print("Total columns:", len(df.columns))

display(df.limit(5))


# COMMAND ----------


# EXPORT GOLD TABLE 1 AS CSV

df = spark.table("workspace.gold.gold_orders")

output_path = "/Volumes/workspace/default/ecommerce_raw/gold_orders_export"

df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("encoding", "UTF-8") \
    .csv(output_path)

print("Gold Orders exported successfully!")
print("Records exported:", df.count())
print("Saved location:", output_path)


# COMMAND ----------


display(dbutils.fs.ls("/Volumes/workspace/default/ecommerce_raw/gold_orders_export"))


# COMMAND ----------


# COPY THE EXPORTED CSV WITH A SIMPLE NAME

folder_path = "/Volumes/workspace/default/ecommerce_raw/gold_orders_export"

files = dbutils.fs.ls(folder_path)

csv_file = [f.path for f in files if f.name.endswith(".csv")][0]

destination = "/Volumes/workspace/default/ecommerce_raw/gold_orders.csv"

dbutils.fs.cp(csv_file, destination)

print("CSV file prepared successfully!")
print("File location:", destination)

# Verify the file
display(dbutils.fs.ls("/Volumes/workspace/default/ecommerce_raw/"))


# COMMAND ----------


# EXPORT REMAINING 6 GOLD TABLES

tables = [
    "gold_daily_sales",
    "gold_product_performance",
    "gold_customer_analytics",
    "gold_payment_analytics",
    "gold_delivery_analytics",
    "gold_review_analytics"
]

base_path = "/Volumes/workspace/default/ecommerce_raw"

for table_name in tables:

    print("\nExporting:", table_name)

    df = spark.table(f"workspace.gold.{table_name}")

    export_folder = f"{base_path}/{table_name}_export"
    final_file = f"{base_path}/{table_name}.csv"

    # Export as a single CSV part file
    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .csv(export_folder)

    # Find the actual CSV part file
    files = dbutils.fs.ls(export_folder)
    csv_file = [f.path for f in files if f.name.endswith(".csv")][0]

    # Copy it with a simple filename
    dbutils.fs.cp(csv_file, final_file)

    print("Records:", df.count())
    print("Saved:", final_file)

print("\n========== ALL 6 EXPORTS COMPLETED ==========")
