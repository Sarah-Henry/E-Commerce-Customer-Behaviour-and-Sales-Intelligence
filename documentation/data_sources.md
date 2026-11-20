
# E-Commerce Customer Behaviour and Sales Intelligence

## 1. Primary Data Source – Olist

Source: Brazilian E-Commerce Public Dataset
Platform: Kaggle

URL: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Format: CSV
Period: 2016–2018

Files collected:
1. Orders
2. Customers
3. Order Items
4. Order Payments
5. Order Reviews
6. Products
7. Sellers
8. Geolocation
9. Product Category Translation

## 2. Secondary Data Source – Open-Meteo

Source: Open-Meteo Historical Weather API

URL: https://archive-api.open-meteo.com/v1/archive

Format: JSON and CSV
Period: 2016–2018
Location: São Paulo, Brazil

Data collected:
- Daily mean temperature
- Daily precipitation

Note: Weather data represents São Paulo only.

## 3. Third Data Source – Nager.Date

Source: Nager.Date Public Holidays API

URL: https://date.nager.at/api/v3/PublicHolidays

Country: Brazil (BR)
Period: 2016–2018
Format: JSON and CSV
Records collected: 42

Data collected:
- Holiday date
- Local holiday name
- English holiday name
- Country code
- Global holiday indicator

## 4. Data Storage

Olist raw CSV files:
data/raw/

Weather and holiday API files:
data/external_api/

## 5. Tools Used

- Python
- Pandas
- Requests
- VS Code

## 6. Data Validation

All 9 Olist CSV files were successfully loaded and inspected using Python Pandas.

Validation included:
- Row and column counts
- Column names
- Missing values
- Duplicate records
- First five records

Original raw datasets are preserved without modification.
