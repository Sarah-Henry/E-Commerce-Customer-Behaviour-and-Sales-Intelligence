# Project Data

This folder contains information about the datasets used in the E-Commerce Customer Behaviour and Sales Intelligence project.

## 1. Main Dataset

**Brazilian E-Commerce Public Dataset by Olist**

Source: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

This dataset contains anonymized Brazilian e-commerce transactions from 2016 to 2018.

## 2. External Data Sources

**Weather Data – Open-Meteo Historical Weather API**

https://archive-api.open-meteo.com/v1/archive

**Public Holidays – Nager.Date API**

https://date.nager.at/api/v3/PublicHolidays

## 3. Local Data Organization

The datasets are stored locally in the following folders:

- `data/raw/` – Original Olist CSV files
- `data/external_api/` – Weather and public holiday API data
- `data/processed/` – Processed datasets

## 4. Important Note

Raw and processed datasets are not uploaded to this public GitHub repository. They are excluded using `.gitignore`.

The Olist dataset is licensed under CC BY-NC-SA 4.0. Please follow its attribution and licence requirements.
