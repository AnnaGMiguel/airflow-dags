from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
import pandas as pd
import re
import os
from datetime import datetime, timedelta
#from datacleaner import data_cleaner

def my_func():
    print('test connection')
    
def data_cleaner():

	airflow_home = os.environ["AIRFLOW_HOME"]
	df = pd.read_csv(airflow_home+'/dags/repo/store_files/raw_store_transactions.csv')
	
	def clean_store_location(st_loc):
        	return re.sub(r'[^\w\s]', '', st_loc).strip()
	
	def clean_product_id(pd_id):
       	 matches = re.findall(r'\d+', pd_id)
        	if matches:
          	  return matches[0]
       	 return pd_id
		
	def remove_dollar(amount):
        	return float(amount.replace('$', ''))
		
	df['STORE_LOCATION'] = df['STORE_LOCATION'].map(lambda x: clean_store_location(x))
	df['PRODUCT_ID'] = df['PRODUCT_ID'].map(lambda x: clean_product_id(x))
	for to_clean in ['MRP', 'CP', 'DISCOUNT', 'SP']:
        	df[to_clean] = df[to_clean].map(lambda x: remove_dollar(x))
	
	df.to_csv(airflow_home+'/dags/repo/store_files/clean_store_transactions.csv', index=False)

 
with DAG('tests_dag', description='Python DAG', schedule_interval=timedelta(1), start_date=datetime(2021, 4, 17), catchup=False) as dag:
	dummy_task 	= DummyOperator(task_id='dummy_task', retries=3)
	python_task	= PythonOperator(task_id='python_task', python_callable=my_func)
	t1=BashOperator(task_id='check_file_exists', bash_command='cat $AIRFLOW_HOME/dags/repo/store_files/raw_store_transactions.csv', retries=2, retry_delay=timedelta(seconds=15))
	t2 = PythonOperator(task_id='clean_raw_csv', python_callable=data_cleaner)
	#t3=BashOperator(task_id='check_new_file', bash_command='cat $AIRFLOW_HOME/dags/repo/store_files/clean_store_transactions.csv', retries=2, retry_delay=timedelta(seconds=15))
	dummy_task >> python_task >> t1 >> t2
