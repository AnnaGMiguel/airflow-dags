from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from datacleaner import data_cleaner

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 4, 18),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('store_dags',default_args=default_args,schedule_interval='@daily', catchup=True) as dag:

    t1=BashOperator(task_id='check_file_exists', bash_command='cat $AIRFLOW_HOME/dags/repo/store_files/raw_store_transactions.csv', retries=2, retry_delay=timedelta(seconds=15))

    t2 = PythonOperator(task_id='clean_raw_csv', python_callable=data_cleaner)

    t1 >> t2