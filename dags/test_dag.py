from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta
from datacleaner import data_cleaner

def my_func():
    print('test connection')
    
 
with DAG('test_dag', description='Python DAG', schedule_interval=timedelta(1), start_date=datetime(2021, 4, 17), catchup=False) as dag:
	dummy_task 	= DummyOperator(task_id='dummy_task', retries=3)
	#python_task	= PythonOperator(task_id='python_task', python_callable=my_func)
	t1=BashOperator(task_id='check_file_exists', bash_command='cat $AIRFLOW_HOME/dags/repo/store_files/raw_store_transactions.csv', retries=2, retry_delay=timedelta(seconds=15))
	t2 = PythonOperator(task_id='clean_raw_csv', python_callable=data_cleaner)
	#t3=BashOperator(task_id='check_new_file', bash_command='cat $AIRFLOW_HOME/dags/repo/store_files/clean_store_transactions.csv', retries=2, retry_delay=timedelta(seconds=15))
	dummy_task >> t1 >> t2
 