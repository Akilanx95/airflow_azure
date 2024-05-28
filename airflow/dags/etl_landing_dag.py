from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

dag = DAG('extract_transform_load_with_spark', default_args=default_args, schedule_interval='@daily')

def get_secret(secret_name):
    keyvault_url = "https://<your-keyvault-name>.vault.azure.net/"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=keyvault_url, credential=credential)
    secret = client.get_secret(secret_name)
    return secret.value

def fetch_metadata():
    postgres_conn_id = "your_postgres_conn_id"
    query = "SELECT * FROM source_meta_table WHERE isActive = 1"
    
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    records = cursor.fetchall()
    
    metadata_list = []
    for record in records:
        metadata = {
            "sno": record[0],
            "table_name": record[1],
            "database": record[2],
            "domain": record[3],
            "classification": record[4],
            "load_type": record[5],
            "last_ingested_time": record[6],
            "keyvault_name": record[7],
            "secret_name": record[8]
        }
        metadata_list.append(metadata)
    
    with open('/dbfs/tmp/metadata_list.json', 'w') as f:
        json.dump(metadata_list, f)

fetch_metadata_task = PythonOperator(
    task_id='fetch_metadata',
    python_callable=fetch_metadata,
    dag=dag
)

databricks_spark_conf = {
    "spark_version": "7.3.x-scala2.12",
    "num_workers": 2,
    "node_type_id": "Standard_D3_v2",
    "spark_conf": {
        "spark.sql.sources.partitionOverwriteMode": "dynamic"
    }
}

notebook_task = {
    "notebook_path": "/IDE/ETL/DataIngestion/SourcetoLanding",
    "base_parameters": {
        "metadata_path": "/dbfs/tmp/metadata_list.json"
    }
}

databricks_task = DatabricksSubmitRunOperator(
    task_id='run_databricks_notebook',
    new_cluster=databricks_spark_conf,
    notebook_task=notebook_task,
    databricks_conn_id='databricks_default',
    dag=dag
)

update_metadata_task = PostgresOperator(
    task_id='update_metadata',
    postgres_conn_id='your_postgres_conn_id',
    sql="UPDATE source_meta_table SET LastIngestedTime = NOW() WHERE isActive = 1",
    dag=dag
)

fetch_metadata_task >> databricks_task >> update_metadata_task
