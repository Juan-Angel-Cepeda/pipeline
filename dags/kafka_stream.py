from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from callables import stream_data_from_api as STMDTA

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,4,21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG('user-automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    streaming_taks = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=STMDTA.stream_data,
    )