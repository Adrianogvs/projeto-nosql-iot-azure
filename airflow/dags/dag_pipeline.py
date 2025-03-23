from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='pipeline_iot_nosql',
    description='Pipeline ETL NoSQL - JSON para Parquet',
    schedule_interval='*/5 * * * *', 
    catchup=False,
    default_args=default_args,
    tags=['iot', 'etl', 'nosql']
) as dag:

    gerar_json = BashOperator(
        task_id='gerar_json',
        bash_command='python /opt/airflow/scripts/producer_eventhub.py'
    )

