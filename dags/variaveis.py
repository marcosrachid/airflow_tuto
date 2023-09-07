from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def print_variable(**kwargs):
    minha_var = Variable.get("minhavar")
    print(f"o valor da variavel é: {minha_var}")


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
    "variaveis",
    description="variaveis",
    default_args=default_args,
    catchup=False,
    default_view="graph",
) as dag:
    task1 = PythonOperator(task_id="tsk1", python_callable=print_variable)
