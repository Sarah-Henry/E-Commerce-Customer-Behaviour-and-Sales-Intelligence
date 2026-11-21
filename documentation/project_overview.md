# E-Commerce Customer Behaviour and Sales Intelligence

## 1. Project Overview

E-Commerce Customer Behaviour and Sales Intelligence is an end-to-end data engineering and analytics project that analyzes customer behaviour, sales performance, product demand, payment patterns and delivery trends.

The project uses Python, Apache Spark and Databricks to extract, process, clean, transform and analyze e-commerce data. The processed data is organized into Bronze, Silver and Gold layers using the Medallion Architecture.

The final analytical datasets are provided to the dashboard team for visualization using Power BI or Tableau.

## 2. Problem Statement

E-commerce platforms generate large amounts of data related to orders, customers, products, payments, reviews and deliveries.

Analyzing these datasets separately makes it difficult to understand overall business performance and customer behaviour.

This project aims to build an end-to-end data pipeline that integrates multiple data sources, transforms raw data into structured analytical datasets and supports meaningful business insights.

## 3. Project Objectives

- Analyze overall sales and revenue trends.
- Understand customer purchasing behaviour.
- Identify high-performing products and categories.
- Analyze payment method distribution.
- Evaluate delivery performance and delays.
- Study customer reviews and satisfaction.
- Integrate external weather and public holiday information.
- Build a reliable data pipeline using Apache Spark and Databricks.
- Prepare Gold-layer datasets for dashboard visualization.

## 4. Dataset Sources

### Main Dataset
Brazilian E-Commerce Public Dataset by Olist.

Source: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

### External Sources
- Open-Meteo Historical Weather API
- Nager.Date Public Holidays API

More details are available in `data_sources.md`.

## 5. Technology Stack

| Technology | Purpose |
|---|---|
| Python | Data extraction and validation |
| Pandas | Local data validation |
| Apache Spark | Large-scale data processing |
| Databricks | Data engineering environment |
| Delta Lake | Structured data storage |
| SQL | Data querying and analysis |
| Power BI / Tableau | Dashboard visualization |
| GitHub | Version control and project documentation |

## 6. Project Architecture

The project follows a three-layer data engineering architecture:

1. Data Extraction: Collect data from Olist and external APIs.
2. Data Engineering: Process data through Bronze, Silver and Gold layers.
3. Data Visualization: Use Gold datasets to build business intelligence dashboards.

Refer to `architecture_diagram.md` for the architecture flowchart.

## 7. Medallion Architecture

### Bronze Layer
Stores the ingested source data in Delta tables.

### Silver Layer
Cleans, standardizes and validates the ingested data.

### Gold Layer
Creates business-ready analytical datasets for reporting and visualization.

## 8. Final Analytical Tables

The project produces seven Gold tables:

1. gold_orders
2. gold_daily_sales
3. gold_product_performance
4. gold_customer_analytics
5. gold_payment_analytics
6. gold_delivery_analytics
7. gold_review_analytics

Detailed descriptions and validation results are available in `gold_tables_documentation.md`.

## 9. Expected Outcomes

- A structured end-to-end data engineering pipeline.
- Clean and validated datasets.
- Business-ready Gold analytical tables.
- Better understanding of e-commerce sales and customer behaviour.
- Interactive business intelligence dashboards.

## 10. Team Contributions

The project is divided into the following responsibilities:

- Member 1: Data extraction and source validation.
- Member 2: Bronze and Silver data engineering.
- Member 3: Gold-layer analytics and data validation.
- Member 4: Dashboard development and visualization.

## 11. Project Status

Data extraction, Bronze ingestion, Silver cleaning and Gold analytical table creation have been completed and validated.

Dashboard development, final report and presentation are part of the remaining project deliverables.
