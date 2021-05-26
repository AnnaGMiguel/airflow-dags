import airflow
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
}

with DAG("pools", default_args=default_args,schedule_interval=timedelta(1)) as dag:
    t1 = BashOperator(task_id="task-1", bash_command="sleep 5",pool ="pool_1")
    t2 = BashOperator(task_id="task-2", bash_command="sleep 5",pool ="pool_1")
    t3 = BashOperator(task_id="task-3", bash_command="sleep 5",pool ="pool_2")
    t4 = BashOperator(task_id="task-4", bash_command="sleep 5",pool ="pool_2")
