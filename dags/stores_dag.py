"""
Code to check a new file exists

"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

from datacleaner import data_cleaner



default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 4, 17),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),

}

dag = DAG("stores_dag", default_args=default_args, schedule_interval=timedelta(1))


t1 = BashOperator(task_id='check_file_exists', bash_command='cat $AIRFLOW_HOME/dags/repo/store_files/raw_store_transactions.csv', retries=2, dag=dag)

t2 = PythonOperator(task_id='clean_raw_csv', python_callable=data_cleaner, dag=dag)

t2.set_upstream(t1)


