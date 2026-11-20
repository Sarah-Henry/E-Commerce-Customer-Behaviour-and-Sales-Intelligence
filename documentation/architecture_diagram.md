# Project Architecture Diagram

## E-Commerce Customer Behaviour and Sales Intelligence

The following diagram represents the end-to-end architecture of the project.

```mermaid
flowchart TD
    A["Olist E-Commerce Dataset"]
    B["Open-Meteo Weather API"]
    C["Nager.Date Holiday API"]

    A --> D["Python Data Extraction"]
    B --> D
    C --> D

    D --> E["Raw Data Storage<br/>Databricks Volume"]

    E --> F["BRONZE LAYER<br/>Raw Delta Tables"]
    F --> G["SILVER LAYER<br/>Data Cleaning and Transformation"]
    G --> H["GOLD LAYER<br/>Business Analytics Tables"]

    H --> I["Power BI / Tableau"]
    I --> J["Interactive Business Dashboard"]

    K["Python / PySpark"]
    L["Databricks"]
    M["Delta Lake"]

    K -.-> D
    L -.-> F
    M -.-> F
    M -.-> G
    M -.-> H
```

## Architecture Explanation

### Layer 1: Data Extraction
- Olist Brazilian E-Commerce dataset.
- Open-Meteo historical weather API for São Paulo.
- Nager.Date Brazilian public holidays API.
- Python scripts for data extraction and validation.

### Layer 2: Data Engineering
- Databricks is used as the data processing environment.
- Apache Spark and PySpark perform transformations.
- Delta Lake stores data in Bronze, Silver and Gold layers.

### Layer 3: Data Visualization
- Gold analytical tables are provided to the dashboard team.
- Power BI or Tableau is used to develop interactive dashboards.
- Dashboards help analyze sales, customers, products, payments, deliveries and reviews.

## Data Flow

Data Sources → Python Extraction → Raw Storage → Bronze → Silver → Gold → Power BI / Tableau Dashboard.
