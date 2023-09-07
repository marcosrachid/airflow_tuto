from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    "depends_on_past": False,
    "start_date": datetime(2023, 3, 5),
    "email": ["teste@teste.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retry": 1,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    "default_args",
    description="default args",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2023, 3, 5),
    catchup=False,
    default_view="graph",
    tags=["processo", "tag", "pipeline"],
) as dag:
    task1 = BashOperator(task_id="tsk1", bash_command="sleep 5", retries=3)
    task2 = BashOperator(task_id="tsk2", bash_command="sleep 5")
    task3 = BashOperator(task_id="tsk3", bash_command="sleep 5")

    task1 >> task2 >> task3
