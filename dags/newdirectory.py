"""
Code to create a new directory

"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta



default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 4, 7),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),

}

dag = DAG("new_directory", default_args=default_args, schedule_interval=timedelta(1))

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = DummyOperator(task_id='dummy_task', retries=3, dag=dag)
#BashOperator(task_id="new_dir", bash_command="mkdir ~/test_direc", dag=dag)
#test if th
#t1 = BashOperator(task_id='check_file_exists', bash_command='cat $AIRFLOW_HOME/dags/repo/store_files/raw_store_transactions.csv', retries=2,retry_delay=timedelta(seconds=15), dag=dag)

t2 = BashOperator(task_id="sleep", bash_command="sleep 5", retries=3, dag=dag)

templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 2)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

t3 = BashOperator(
    task_id="templated",
    bash_command=templated_command,
    params={"my_param": "New directory"},
    dag=dag,
)

t2.set_upstream(t1)
t3.set_upstream(t1)
