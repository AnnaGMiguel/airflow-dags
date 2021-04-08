"""
Code to create a new directory

"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 4, 7),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

def create_dir():
   os.makedirs("test_dir")
   print("created") 

dag = DAG("new_directory", default_args=default_args, schedule_interval=timedelta(1))

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)

t2 = BashOperator(task_id="sleep", bash_command="sleep 5", retries=3, dag=dag)

t3 = PythonOperator(task_id="create_dir", python_callable=create_dir, dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t1)
