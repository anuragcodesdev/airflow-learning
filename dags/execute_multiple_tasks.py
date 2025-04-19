from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
import os

# Get directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Point to 'bash_scripts' folder inside the same directory
template_searchpath = os.path.join(script_dir, 'bash_scripts')

# Default arguments for the DAG
default_args = {
    'owner': 'anuragthakur',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Define the DAG
with DAG(
    dag_id='executing_multiple_tasks',
    description='DAG with multiple tasks and dependencies.',
    default_args=default_args,
    start_date=pendulum.now("UTC").subtract(days=1),
    schedule_interval='@once',
    tags=['beginner', 'bash', 'hello world'],
    template_searchpath=template_searchpath
) as dag:

    taskA = BashOperator(
        task_id='taskA',
        bash_command='taskA.sh'
    )

    taskB = BashOperator(
        task_id='taskB',
        bash_command='taskB.sh'
    )

    taskC = BashOperator(
        task_id='taskC',
        bash_command='taskC.sh'
    )

    taskD = BashOperator(
        task_id='taskD',
        bash_command='taskD.sh'
    )
    
    taskE = BashOperator(
        task_id='taskE',
        bash_command='taskE.sh'
    )

    # Set task dependencies
    taskA >> [taskB, taskC]  # taskB and taskC depend on taskA
    [taskB, taskC] >> taskD  # taskD depends on taskB and taskC
    taskD >> taskE
