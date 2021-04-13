from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner' : 'Airflow',
    'start_date' : datetime(2021, 4, 13),
    'retries' : 1,
    'retry_delay' : timedelta(seconds=5)
}

dag = DAG('store_dag', default_args = default_args, schedule_interval = '@daily', catchup = False)

t1 = BashOperator(task_id = 'check_file_exists', bash_command = 'shasum ~ /store_files/raw_store_transactions.csv', retries = 2,
 retry_delay = timedelta(seconds=15), dag = dag)