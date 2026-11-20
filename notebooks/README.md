# Databricks Notebooks

This folder contains the three notebooks used to build the project's Medallion Architecture.

## Notebooks

1. **03_Bronze_Data_Ingestion.py** – Loads raw Olist and external API data into Bronze Delta tables.
2. **04_Silver_Data_Cleaning.py** – Cleans, transforms and validates the data in the Silver layer.
3. **05_Gold_Data_Engineering.py** – Creates analytical Gold tables for business intelligence and dashboard development.

## Execution Environment

- Databricks
- Python
- Apache Spark (PySpark)
- Delta Lake

The notebooks are designed to run in the project's Databricks workspace after the required source data has been uploaded.
