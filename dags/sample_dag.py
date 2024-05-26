from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
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

def save_sap_credentials_to_dbfs():
    postgres_conn_id = "your_postgres_conn_id"
    query = "SELECT * FROM source_meta_table WHERE isActive = 1"
    
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    records = cursor.fetchall()
    
    for record in records:
        credentials = {
            "host": get_secret(record[7]),
            "port": get_secret(record[7]),
            "user": get_secret(record[7]),
            "password": get_secret(record[7])
        }
        
        with open(f'/dbfs/tmp/{record[1]}_credentials.json', 'w') as f:
            json.dump(credentials, f)

save_sap_credentials_task = PythonOperator(
    task_id='save_sap_credentials',
    python_callable=save_sap_credentials_to_dbfs,
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
        "input_path": "/dbfs/tmp/",
        "output_path": "/dbfs/tmp/transformed_data.parquet"
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

save_sap_credentials_task >> databricks_task >> update_metadata_task
