import requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta


def query_api():
    response = requests.get("https://api.publicapis.org/entries")
    print(response)


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
    "sensors",
    description="sensors",
    default_args=default_args,
    catchup=False,
    default_view="graph",
) as dag:
    check_api = HttpSensor(
        task_id="check_api",
        http_conn_id="myconnection",
        endpoint="entries",
        poke_interval=5,
        timeout=20,
    )

    process_data = PythonOperator(task_id="process_data", python_callable=query_api)

    check_api >> process_data
