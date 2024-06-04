from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': ['info@tiger.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'etl_master_dag',
    default_args=default_args,
    description='Master DAG to orchestrate child DAGs',
    schedule_interval='@daily',
)

start_landing_dag = TriggerDagRunOperator(
    task_id='start_landing_dag',
    trigger_dag_id='etl_landing_dag',
    dag=dag,
)

start_bronze_dag = TriggerDagRunOperator(
    task_id='start_bronze_dag',
    trigger_dag_id='etl_bronze_dag',
    dag=dag,
)

start_silver_dag = TriggerDagRunOperator(
    task_id='start_silver_dag',
    trigger_dag_id='etl_silver_dag',
    dag=dag,
)

start_gold_dag = TriggerDagRunOperator(
    task_id='start_gold_dag',
    trigger_dag_id='etl_gold_dag',
    dag=dag,
)

# Define the sequence
start_landing_dag >> start_bronze_dag >> start_silver_dag >> start_gold_dag
