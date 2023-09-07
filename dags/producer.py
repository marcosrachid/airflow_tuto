import pandas as pd

from airflow import DAG, Dataset
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def my_file():
    dataset = pd.read_csv("/opt/airflow/data/Churn.csv", sep=";")
    dataset.to_csv("/opt/airflow/data/Churn_new.csv", sep=";")


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
    "producer",
    description="producer",
    default_args=default_args,
    catchup=False,
    default_view="graph",
) as dag:
    mydataset = Dataset("/opt/airflow/data/Churn_new.csv")

    t1 = PythonOperator(task_id="t1", python_callable=my_file, outlets=[mydataset])
