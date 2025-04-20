import pendulum

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import PythonOperator

default_args = {
    'owner':'anuragthakur'
}

def print_function():
    print("Python code is ran!")
    
with DAG(
    dag_id = 'execute_python_operators',
    description = 'Python operators in DAGs',
    default_args = default_arg,
    start_date = pendulum.now("UTC").subtract(days=1),
    schedule_interval = '@daily',
    tags = ['simple', 'python']
) as dag:
    task = PythonOperator(
        task_id = 'python_task'
    )