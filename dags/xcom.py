from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def task_write(**kwargs):
    kwargs["ti"].xcom_push(key="valorxcom1", value=10200)


def task_read(**kwargs):
    valor = kwargs["ti"].xcom_pull(key="valorxcom1")
    print(f"valor recuperado: {valor}")


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
    "xcom",
    description="xcom",
    default_args=default_args,
    catchup=False,
    default_view="graph",
) as dag:
    task1 = PythonOperator(task_id="tsk1", python_callable=task_write)
    task2 = PythonOperator(task_id="tsk2", python_callable=task_read)

    task1 >> task2
