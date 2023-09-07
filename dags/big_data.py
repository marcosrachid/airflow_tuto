from airflow import DAG
from big_data_operator import BigDataOperator
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
    "big_data",
    description="big_data",
    default_args=default_args,
    catchup=False,
    default_view="graph",
) as dag:
    big_data_parquet = BigDataOperator(
        task_id="big_data_parquet",
        path_to_csv_file="/opt/airflow/data/Churn.csv",
        path_to_save_file="/opt/airflow/data/Churn.parquet",
    )

    big_data_json = BigDataOperator(
        task_id="big_data_json",
        path_to_csv_file="/opt/airflow/data/Churn.csv",
        path_to_save_file="/opt/airflow/data/Churn.json",
        file_type="json",
    )
