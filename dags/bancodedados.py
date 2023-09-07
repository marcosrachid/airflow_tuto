from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta


def print_result(ti):
    task_instance = ti.xcom_pull(task_ids="query_data")
    print("Resultado da consulta: ")
    for row in task_instance:
        print(row)


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
    "bancodedados",
    description="bancodedados",
    default_args=default_args,
    catchup=False,
    default_view="graph",
) as dag:
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres",
        sql="create table if not exists teste(id int);",
    )

    insert_data = PostgresOperator(
        task_id="insert_data",
        postgres_conn_id="postgres",
        sql="insert into teste values (1);",
    )

    query_data = PostgresOperator(
        task_id="query_data",
        postgres_conn_id="postgres",
        sql="select * from teste;",
    )

    print_result_task = PythonOperator(
        task_id="print_result_task", python_callable=print_result, provide_context=True
    )

    create_table >> insert_data >> query_data >> print_result_task
