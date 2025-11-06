from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "melvin",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="SCD2_snapshots",
    default_args=default_args,
    description="Run dbt snapshots for SCD2",
    schedule_interval="@daily",    
    start_date=datetime(2025, 9, 1),
    catchup=False,
    tags=["dbt", "snapshots"],
) as dag:
    
    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /opt/airflow/bank && dbt run --select staging --profiles-dir /home/airflow/.dbt"
    )
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="cd /opt/airflow/bank && dbt snapshot --profiles-dir /home/airflow/.dbt"
        )
    dbt_run_analytics = BashOperator(
        task_id="dbt_run_analytics",
        bash_command="cd /opt/airflow/bank && dbt run --select analytics --profiles-dir /home/airflow/.dbt"
    )

    dbt_staging >> dbt_snapshot >> dbt_run_analytics