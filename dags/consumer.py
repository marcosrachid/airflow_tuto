import pandas as pd

from airflow import DAG, Dataset
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def my_file():
    dataset = pd.read_csv("/opt/airflow/data/Churn_new.csv", sep=";")
    dataset.to_csv("/opt/airflow/data/Churn_new_2.csv", sep=";")


mydataset = Dataset("/opt/airflow/data/Churn_new.csv")

with DAG(
    "consumer",
    description="consumer",
    schedule=[mydataset],
    start_date=datetime(2023, 3, 5),
    catchup=False,
    default_view="graph",
) as dag:
    t1 = PythonOperator(task_id="t1", python_callable=my_file, provide_context=True)
