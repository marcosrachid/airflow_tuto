import pandas as pd
import statistics as sts

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def data_cleaner():
    dataset = pd.read_csv("/opt/airflow/data/Churn.csv", sep=";")
    dataset.columns = [
        "id",
        "score",
        "estado",
        "genero",
        "idade",
        "patrimonio",
        "saldo",
        "produtos",
        "tem_card_credito",
        "ativo",
        "salario",
        "saiu",
    ]

    mediana = sts.median(dataset["salario"])
    dataset["salario"].fillna(mediana, inplace=True)
    dataset["genero"].fillna("Masculino", inplace=True)

    mediana = sts.median(dataset["idade"])
    dataset.loc[(dataset["idade"] < 0) | (dataset["idade"] > 120), "idade"] = mediana

    dataset.drop_duplicates(subset="id", keep="first", inplace=True)

    dataset.to_csv("/opt/airflow/data/Churn_Clean.csv", sep=";", index=False)


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
    "pythonoperator",
    description="pythonoperator",
    default_args=default_args,
    catchup=False,
    default_view="graph",
) as dag:
    t1 = PythonOperator(task_id="t1", python_callable=data_cleaner)
