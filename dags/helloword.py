"""
  CODE TO PRINT A HELLO WORD USING PYTHON OPERATOR
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import datetime, timedelta

def my_func():
    print('Hello from airflow')
 
with DAG('python_dag', description='Python DAG', schedule_interval=timedelta(1), start_date=datetime(2021, 4, 9), catchup=False) as dag:
	dummy_task 	= DummyOperator(task_id='dummy_task', retries=3)
	python_task	= PythonOperator(task_id='python_task', python_callable=my_func)
 
	dummy_task >> python_task
