from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

with DAG(
    "task_group_1",
    description="task_group",
    schedule_interval=None,
    start_date=datetime(2023, 3, 5),
    catchup=False,
) as dag:
    task1 = BashOperator(task_id="tsk1", bash_command="sleep 5")
    task2 = BashOperator(task_id="tsk2", bash_command="sleep 5")

    tsk_group = TaskGroup("tsk_group")

    task3 = BashOperator(task_id="tsk3", bash_command="sleep 5", task_group=tsk_group)
    task4 = BashOperator(task_id="tsk4", bash_command="sleep 5", task_group=tsk_group)
    task5 = BashOperator(task_id="tsk5", bash_command="sleep 5", task_group=tsk_group)
    task6 = BashOperator(task_id="tsk6", bash_command="sleep 5", task_group=tsk_group)

    [task1, task2] >> tsk_group
