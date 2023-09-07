import random

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta


def gera_numero_aleatorio():
    return random.randint(1, 100)


def avalia_numero_aleatorio(**context):
    number = context["task_instance"].xcom_pull(task_ids="gera_numero_aleatorio_task")
    if number % 2:
        return "par_task"
    return "impar_task"


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
    "branch",
    description="branch",
    default_args=default_args,
    catchup=False,
    default_view="graph",
) as dag:
    gera_numero_aleatorio_task = PythonOperator(
        task_id="gera_numero_aleatorio_task", python_callable=gera_numero_aleatorio
    )

    avalia_numero_aleatorio_task = BranchPythonOperator(
        task_id="avalia_numero_aleatorio_task",
        python_callable=avalia_numero_aleatorio,
        provide_context=True,
    )

    par_task = BashOperator(task_id="par_task", bash_command='echo "numero par"')
    impar_task = BashOperator(
        task_id="impar_task",
        bash_command='echo "numero impar"',
    )

    gera_numero_aleatorio_task >> avalia_numero_aleatorio_task
    avalia_numero_aleatorio_task >> par_task
    avalia_numero_aleatorio_task >> impar_task
