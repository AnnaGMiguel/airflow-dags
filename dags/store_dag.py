from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from datacleaner import data_cleaner

default_args = {
    'owner' : 'airflow',
    'start_date' : datetime(2021, 4, 13),
    'retries' : 1,
    'retry_delay' : timedelta(seconds=5)
}

dag = DAG('store_dag', default_args=default_args, schedule_interval=timedelta(1))# schedule_interval='@daily', catchup=False)

t1 = BashOperator(task_id='check_file_exists', bash_command='shasum $AIRFLOW__CORE__DAGS_FOLDER/raw_store_transactions.csv', retries=2,
 retry_delay=timedelta(seconds=15), dag=dag)

t2 = PythonOperator(task_id='clean_raw_csv', python_callable=data_cleaner, dag=dag)