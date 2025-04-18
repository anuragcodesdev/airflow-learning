from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

default_args = {
    'owner' : 'anuragthakur'
}

with DAG(
    dag_id = 'executing_multiple_tasks',
    description = 'DAG with multiple tasks and dependencies.',
    default_args = default_args,
    start_date = pendulum.now("UTC").subtract(days=1),
    schedule_interval = '@once',
    tags = ['beginner', 'bash', 'hello world']
) as dag:
    
#     taskA = BashOperator(
#         task_id = 'taskA',
#         bash_command = 'Echo TASK A has executed!'
#     )
    
#     taskB = BashOperator(
#         task_id = 'taskB',
#         bash_command = 'echo TASK B has executed'
#     )
    
# Working with up and down stream made easier.
    taskA = BashOperator(
        task_id = 'taskA',
        bash_command = '''
        echo TASK A has started!
        
        for i in {1...10}
        do
            echo TASK A printing $i
        done
        
        echo TASK A ended!
    '''
    )
    
    taskB = BashOperator(
        task_id = 'taskB',
        bash_command = '''
        echo TASK B has started!
        sleep 4
        echo TASK B has ended!
    '''
    )
    
    taskC = BashOperator(
        task_id = 'taskC',
        bash_command =  '''
        echo TASK C has started!
        sleep 15
        echo TASK C has ended!
    '''
    )

    taskD = BashOperator(
        task_id = 'taskD',
        bash_command = 'echo TASK D completed!'
    )
    

taskA >> [taskB, taskC] #taskB and taskC depend on taskA
taskD << [taskB, taskC] #taskB and taskC depend on task D