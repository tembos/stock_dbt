import requests
import json
import uuid

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import pandas as pd
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


from google.cloud import secretmanager
import pendulum

SYMBOL = "NVDA"

# GCP variables
BIGQUERY_PROJECT = "bigquerysheets-404104"
GCS_BUCKET = "4353453453_data_stocks_scotia_project"

# metadata ingestion
batch_id = str(uuid.uuid4())  # Unique batch ID for tracking
ingestion_datetime = pendulum.now("UTC").to_datetime_string()

# secret
client = secretmanager.SecretManagerServiceClient()
secret_path = f"projects/{BIGQUERY_PROJECT}/secrets/apikey/versions/latest"
response_secret = client.access_secret_version(name=secret_path)
API_KEY = response_secret.payload.data.decode("UTF-8")

# big query variables
BIGQUERY_DATASET = "stocks_raw"
BIGQUERY_TABLE = f"source_{SYMBOL.lower()}"
BQ_TABLE_PATH = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"


dag = DAG(
    f"fetch_process_store_{SYMBOL.lower()}_data",
    description="Fetch stock data as JSON, process to Parquet, store in GCS, create BQ table if needed, and load into BigQuery",
    schedule_interval= "0 9 * * 1-5",
    start_date=pendulum.datetime(2025, 2, 6),
    catchup=True,
)


def fetch_and_store_parquet(**context):
    PROCESSED_PARQUET_PATH = f"processed/{SYMBOL}_data_{context['ds']}.parquet"

    base_url = "https://api.twelvedata.com/time_series"

    params = {
        "apikey": API_KEY,
        "interval": "1min",
        "format": "JSON",
        "start_date": f"{context['ds']} 00:00:00",
        "end_date": f"{context['ds']} 23:59:00",
        "symbol": SYMBOL,
    }
    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        data = response.json()
        values_list = data.get("values", [])
        meta_struct = data.get("meta", {})
        df_values = pd.DataFrame(values_list)

        df_values["meta"] = json.dumps(meta_struct)
        df_values["ingestion_datetime_utc"] = ingestion_datetime
        df_values["batch_id"] = batch_id

        local_parquet_file = f"/tmp/{SYMBOL}_{context['ds']}.parquet"
        df_values.to_parquet(local_parquet_file, engine="pyarrow")

        gcs_hook = GCSHook()
        gcs_hook.upload(bucket_name=GCS_BUCKET, object_name=PROCESSED_PARQUET_PATH, filename=local_parquet_file)

        print(f"Processed Parquet stored in gs://{GCS_BUCKET}/{PROCESSED_PARQUET_PATH}")
        return PROCESSED_PARQUET_PATH

    else:
        error_message = f"Error fetching data: {response.status_code}, {response.text}"
        raise AirflowException(error_message)


create_bq_table_task = BigQueryInsertJobOperator(
    task_id="create_bq_table",
    project_id=BIGQUERY_PROJECT,
    configuration={
        "query": {
            "query": f"""
            CREATE SCHEMA IF NOT EXISTS `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}`;
                CREATE TABLE IF NOT EXISTS `{BQ_TABLE_PATH}` (
                    datetime STRING,
                    `open` STRING,
                    high STRING,
                    low STRING,
                    `close` STRING,
                    volume STRING,
                    meta STRING,
                    ingestion_datetime_utc STRING,
                    batch_id STRING
                );
            """,
            "useLegacySql": False,
        }
    },
    dag=dag,
)

api_to_gcs_task = PythonOperator(
    task_id="fetch_parquet_task",
    python_callable=fetch_and_store_parquet,
    dag=dag,
    provide_context=True,
)

gcs_to_bigquery_task = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery",
    bucket=GCS_BUCKET,
    source_objects=["processed/NVDA_data_{{ ds }}.parquet"],
    destination_project_dataset_table=BQ_TABLE_PATH,
    source_format="PARQUET",
    write_disposition="WRITE_APPEND",
    dag=dag,
)

run_dbt = KubernetesPodOperator(
    namespace='composer-user-workloads',
    image='gcr.io/bigquerysheets-404104/dbt-bigquery:latest',
    cmds=["dbt"],
    arguments=["run", "--warn-error", "--select", f"tag:{SYMBOL.lower()}"],
    name=f'run-dbt-{SYMBOL.lower()}',
    task_id=f'run_dbt_task_{SYMBOL.lower()}',
    get_logs=True,
    in_cluster=True,
    is_delete_operator_pod=True,
)

create_bq_table_task >> api_to_gcs_task >> gcs_to_bigquery_task>>run_dbt
