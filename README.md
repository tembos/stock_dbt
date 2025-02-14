# composer_dbt

1. Directories of the project
- dir Airflow-Composer

  - roles : roles for airflow to access GKE's namespace
  - dags : dags that are copied in composer's GCS bucket

- dir dbt
   - stocks_use_case : dbt project
   - Dockerfile : image where dbt is running on GKE's Pod

2. About the project

  This is a use case for DBT with Composer GCP

    A. Ingestion and data storage

  Stocks market data are pulled from an API via a DAG into GCS as parquet then exported toBigQuery

    B. Data transformation and modelling

   DBT manages BigQuery models, from the raw data to the data warehouse

  
3. Environment, process and tests

#tbd