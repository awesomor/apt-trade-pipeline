from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DBT_DIR = "/opt/airflow/dbt/apt_mart"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="apt_trade_dbt",
    default_args=default_args,
    schedule_interval=None,      # TriggerDagRunOperator로만 실행
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["apt", "dbt"],
) as dag:

    run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_DIR} && dbt run --select staging --profiles-dir .",
    )

    run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_DIR} && dbt run --select marts --profiles-dir .",
    )

    run_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir .",
    )

    run_staging >> run_marts >> run_test
