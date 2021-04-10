"""
  CODE TO PRINT A HELLO WORD USING PYTHON OPERATOR
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import datetime, timedelta

def my_func(**kwargs):
    print('Hello from airflow')
    return kwargs['param_1']
 
with DAG('python_dag', description='Python DAG', schedule_interval=timedelta(1), start_date=datetime(2021, 4, 9), catchup=False) as dag:
	dummy_task 	= DummyOperator(task_id='dummy_task', retries=3)
	python_task	= PythonOperator(task_id='python_task', python_callable=my_func, op_kwargs={'param_1': 'One', 'param_2': 'Two', 'param_3': 'Three'})
 
	dummy_task >> python_task
