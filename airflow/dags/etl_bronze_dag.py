import json
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.email_operator import EmailOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': ['info@tiger.com'],
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'LANDING_TO_BRONZE_DAG',
    default_args=default_args,
    description='DAG to move data from Landing to Bronze zone',
    schedule_interval='@daily',
)

def fetch_metadata():
    postgres_hook = PostgresHook(postgres_conn_id='your_postgres_conn_id')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM dbo.meta_data WHERE is_active = 1 AND Layer = 'LandingToBronze'")
    records = cursor.fetchall()
    metadata_list = []
    for record in records:
        metadata = {
            "sno": record[0],
            "table_name": record[1],
            "database": record[2],
            "domain": record[3],
            "classification": record[4],
            "merge_strategy": record[5],
            "merge_key": record[6],
            "watermark_column": record[7],
            "last_ingested_time": record[8],
            "keyvault_name": record[9],
            "secret_name": record[10],
            "frequency": record[11],
            "source_system": record[12]
        }
        metadata_list.append(metadata)
    return metadata_list

def notify_teams(success, metadata):
    webhook_url = 'https://outlook.office.com/webhook/YOUR_TEAMS_WEBHOOK_URL'
    if success:
        message = f"Data Ingestion Succeeded for table: {metadata['table_name']}. Total Rows Processed: {metadata['Total_row_count']}"
    else:
        message = f"Data Ingestion Failed for table: {metadata['table_name']}. Check the logs for more details."
    payload = {
        "text": message
    }
    requests.post(webhook_url, json=payload)

def insert_log(metadata, success):
    postgres_hook = PostgresHook(postgres_conn_id='your_postgres_conn_id')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    insert_query = """
    INSERT INTO dbo.ingestion_log (table_name, database, domain, classification, Layer, Total_row_count, Records_inserted, Records_updated, Records_deleted, timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(insert_query, (
        metadata['table_name'],
        metadata['database'],
        metadata['domain'],
        metadata['classification'],
        'bronze',
        metadata['Total_row_count'],
        metadata['Records_inserted'],
        metadata['Records_updated'],
        metadata['Records_deleted'],
        datetime.now()
    ))
    connection.commit()

def process_metadata():
    metadata_list = fetch_metadata()
    for metadata in metadata_list:
        try:
            run_databricks_notebook(metadata)
            notify_teams(True, metadata)
            insert_log(metadata, True)
        except Exception as e:
            notify_teams(False, metadata)
            insert_log(metadata, False)
            raise e

def run_databricks_notebook(metadata):
    databricks_spark_conf = {
        "spark_version": "7.3.x-scala2.12",
        "num_workers": 2,
        "node_type_id": "Standard_D3_v2",
        "spark_conf": {
            "spark.sql.sources.partitionOverwriteMode": "dynamic"
        }
    }
    notebook_task = {
        "notebook_path": "workspace/IDE/ETL/DataIngestion/LandingtoBronze.py",
        "base_parameters": {
            "metadata": json.dumps(metadata)
        }
    }
    run_notebook = DatabricksSubmitRunOperator(
        task_id=f"run_databricks_notebook_{metadata['table_name']}",
        new_cluster=databricks_spark_conf,
        notebook_task=notebook_task,
        databricks_conn_id='databricks_default',
        dag=dag
    )
    return run_notebook

start_task = PythonOperator(
    task_id='start',
    python_callable=process_metadata,
    dag=dag
)

start_task
