from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    "depends_on_past": False,
    "start_date": datetime(2023, 3, 5),
    "schedule_interval": "@once",
    "email": ["teste@teste.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retry": 1,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    "pool",
    description="pool",
    default_args=default_args,
    catchup=False,
    default_view="graph",
) as dag:
    task1 = BashOperator(task_id="tsk1", bash_command="sleep 5", pool="meupool")
    task2 = BashOperator(
        task_id="tsk2", bash_command="sleep 5", pool="meupool", priority_weight=5
    )
    task3 = BashOperator(task_id="tsk3", bash_command="sleep 5", pool="meupool")
    task4 = BashOperator(
        task_id="tsk4", bash_command="sleep 5", pool="meupool", priority_weight=10
    )
