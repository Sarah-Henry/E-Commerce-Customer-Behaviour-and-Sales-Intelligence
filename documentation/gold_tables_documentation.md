# Gold Layer – Table Documentation

The Gold layer contains seven analytical tables created using PySpark and Delta Lake in Databricks.

## 1. gold_orders
- Records: 99,441
- Purpose: One consolidated record per order.
- Contains order status, customer details, item count, revenue, freight, payments and reviews.

## 2. gold_daily_sales
- Records: 634
- Purpose: Daily sales performance analysis.
- Contains daily orders, revenue, freight, payments, average reviews, delivery status, weather and holidays.

## 3. gold_product_performance
- Records: 32,951
- Purpose: Product-level performance analysis.
- Contains product categories, units sold, product revenue, delivered units and revenue.

## 4. gold_customer_analytics
- Records: 96,096
- Purpose: Customer purchasing behaviour analysis.
- Repeat customers: 2,997
- First-time customers: 93,099

## 5. gold_payment_analytics
- Records: 5
- Purpose: Payment preference analysis.
- Contains payment types and transaction counts.

## 6. gold_delivery_analytics
- Records: 27
- Purpose: Delivery performance analysis across Brazilian states.
- Contains delivery times and delays.

## 7. gold_review_analytics
- Records: 27
- Purpose: Customer satisfaction analysis.
- Contains review counts, average scores and star-rating counts.

## Validation Summary

All seven Gold tables were successfully created.

All nine validation checks passed:

1. Gold order count matches Silver orders.
2. No duplicate orders.
3. Daily order totals match.
4. Product units match.
5. Product revenue matches.
6. Payment transaction counts match.
7. Review record counts match.
8. No duplicate unique customers.
9. Delivery records match.
