from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum


default_args = {
    'owner' : 'anuragthakur',
}

dag = DAG(
    dag_id = 'hello_world',
    description = 'Our first "Hello World" DAG!',
    default_args = default_args,
    start_date = pendulum.now("UTC").subtract(days=1),
    schedule_interval = '@daily',
    tags = ['beginner', 'bash', 'hello world']
)

task = BashOperator(
    task_id = 'hello_world_task',
    bash_command = 'echo Hello world once again!!',
    dag = dag
)

# Alternative initialisation

with DAG(
    dag_id = 'hello_world',
    description = 'Our first "Hello World" DAG!',
    default_args = default_args,
    start_date = pendulum.now("UTC").subtract(days=1),
    schedule_interval = '@daily',
    tags = ['beginner', 'bash', 'hello world']
) as dag:
    
    task = BashOperator(
        task_id = 'hello_world_task',
        bash_command = 'echo Hello world once again using "with"!!'
    )

task