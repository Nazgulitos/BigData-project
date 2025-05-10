# Scripts Directory

This folder contains various scripts used in the project. These scripts are responsible for automating specific tasks across the four main stages of the Big Data pipeline: data ingestion, preprocessing, model training, and evaluation. Below is a detailed breakdown of each stage and the scripts used:

## Stage 1 - Data Engineering
This stage focuses on collecting, preprocessing, and storing raw data for further analysis.
- `data_collection.sh`: Automates the process of collecting raw data from kaggel source.
- `data_preprocess.py`: Cleans and preprocesses the raw data, handling missing values and inconsistencies.
- `data_storage.sh`: Stores the processed data into a structured format for easy access.

## Stage 2 - EDA and Hive Optimization
This stage involves exploratory data analysis (EDA) and optimizing data storage for efficient querying.
- `eda_queries.sh`: Calls hql queries, puts data for further using in Apache Superset.

## Stage 3 - Model Training
This stage is dedicated to training machine learning models using the preprocessed data.
- `ml_pipeline.py`: Implements the entire machine learning pipeline, including data loading, model training, and saving the trained models.

## Stage 4 - Superset Dashboard Preparation
This stage prepares the data for visualization and reporting using Superset dashboards.