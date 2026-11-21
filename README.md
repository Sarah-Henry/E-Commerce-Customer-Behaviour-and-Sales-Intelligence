# E-Commerce Customer Behaviour and Sales Intelligence

## 1. Project Overview

E-Commerce Customer Behaviour and Sales Intelligence is an end-to-end data engineering and analytics project that analyzes e-commerce sales, customer behaviour, product performance, payment preferences, delivery trends and customer reviews.

The project uses Python, Apache Spark, Databricks, Delta Lake and Power BI/Tableau to transform raw data into meaningful business insights.

## 2. Problem Statement

E-commerce businesses generate large amounts of data from customer orders, products, payments, reviews and deliveries.

This project aims to process and analyze this data to identify sales patterns, understand customer purchasing behaviour, evaluate product performance and discover factors affecting delivery and customer satisfaction.

## 3. Technologies Used

- Python
- Apache Spark (PySpark)
- Databricks
- Delta Lake
- SQL
- Pandas
- REST APIs
- Tableau (Dashboard)

## 4. Data Sources

### Main Dataset
Brazilian E-Commerce Public Dataset by Olist.

Source: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

The dataset contains historical Brazilian e-commerce records from 2016–2018.

### External Data Sources

**Weather Data – Open-Meteo API**

Source: https://archive-api.open-meteo.com/v1/archive

Historical weather data for São Paulo, Brazil.

**Public Holidays – Nager.Date API**

Source: https://date.nager.at/

Brazilian public holiday information for 2016–2018.

## 5. Project Architecture

The project follows a three-layer data engineering architecture:

1. **Data Extraction:** Collecting the Olist dataset and external API data using Python.
2. **Data Engineering:** Processing data in Databricks using PySpark and Delta Lake through Bronze, Silver and Gold layers.
3. **Data Visualization:** Connecting Gold analytical tables to Power BI/Tableau for dashboard development.

## 6. Medallion Architecture

### Bronze Layer
- Ingests raw CSV data into Delta tables.
- Maintains source data for further processing.

### Silver Layer
- Handles duplicate records.
- Performs data cleaning and type conversions.
- Handles missing and inconsistent values.
- Creates cleaned analytical datasets.

### Gold Layer
Creates seven analytical tables:

1. gold_orders
2. gold_daily_sales
3. gold_product_performance
4. gold_customer_analytics
5. gold_payment_analytics
6. gold_delivery_analytics
7. gold_review_analytics

## 7. Key Project Results

- 99,441 orders processed.
- 96,096 unique customers analyzed.
- 112,650 order items processed.
- 1,096 daily weather records collected.
- 42 Brazilian public holiday records collected.
- Seven Gold analytical tables created.
- Nine final Gold validation checks passed.

## 8. Repository Structure

```text
E-Commerce-Customer-Behaviour-and-Sales-Intelligence/
│
├── README.md
├── .gitignore
│
├── extraction/
│   ├── validate_data.py
│   └── extract_external_data.py
│
├── documentation/
│   └── data_sources.md
│
├── notebooks/
│   ├── 03_Bronze_Data_Ingestion
│   ├── 04_Silver_Data_Cleaning
│   └── 05_Gold_Data_Engineering
│
├── dashboard/
│
└── reports/
```

## 9. Team Contributions

The project is developed collaboratively.

- **Member 1:** Data extraction and external API integration.
- **Member 2:** Bronze and Silver data engineering.
- **Member 3:** Gold layer development and analytical transformations.
- **Member 4:** Dashboard development and project documentation/presentation integration.

## 10. Dataset and Licensing Notice

The Olist dataset is distributed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International (CC BY-NC-SA 4.0) licence.

The original dataset is not included in this repository. Please obtain it from the original Kaggle source and follow its licence terms.

Weather and public holiday data are obtained from their respective API providers.

## 11. Project Status

- [x] Data extraction
- [x] External API integration
- [x] Bronze layer
- [x] Silver layer
- [x] Gold layer
- [x] Gold data validation
- [ ] Dashboard development
- [ ] Final project report
- [ ] Final presentation

---

**Project:** E-Commerce Customer Behaviour and Sales Intelligence

**Purpose:** Turning e-commerce data into meaningful business insights.


---

## 📚 Project Documentation

Explore the different parts of the project:

| Documentation | Description |
|---|---|
| [Project Overview](documentation/project_overview.md) | Project objectives, problem statement and technology stack |
| [Data Sources](documentation/data_sources.md) | Details of datasets and external APIs |
| [Architecture Diagram](documentation/architecture_diagram.md) | Complete data pipeline architecture |
| [Gold Tables Documentation](documentation/gold_tables_documentation.md) | Gold tables, descriptions and validation |
| [Data Folder](data/README.md) | Dataset organization and download information |
| [Extraction Scripts](extraction/) | Python data extraction and validation scripts |
| [Databricks Notebooks](notebooks/) | Bronze, Silver and Gold notebooks |
| [Dashboard](dashboard/README.md) | Dashboard development information |
| [Screenshots](documentation/screenshots/) | Evidence of successful pipeline execution |

## 📸 Pipeline Execution Evidence

Screenshots of the successful Bronze ingestion, Silver cleaning and Gold validation processes are available in the [screenshots folder](documentation/screenshots/).

## 📌 Current Project Status

- [x] Data extraction and validation
- [x] Bronze layer ingestion
- [x] Silver layer cleaning
- [x] Gold layer analytics
- [x] Gold table validation
- [x] Project documentation
- [ ] Power BI / Tableau dashboard
- [ ] Final project report
- [ ] Final presentation
