from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

default_args = {
    "owner": "melvin",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="DBT_Bank_Data_Transformation_Pipeline",
    default_args=default_args,
    description="Run dbt snapshots for SCD2",
    schedule_interval=None,    
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=["dbt", "scd2", "snowflake"],
) as dag:
    
    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="""
        cd /opt/airflow/bank \
        && dbt run --select staging --profiles-dir /home/airflow/.dbt \
        && dbt test --select staging --profiles-dir /home/airflow/.dbt || exit 0
        """    
    )
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="""
        cd /opt/airflow/bank \
        && dbt snapshot --profiles-dir /home/airflow/.dbt"""
    )
    dbt_run_analytics = BashOperator(
        task_id="dbt_run_analytics",
        bash_command="""
        cd /opt/airflow/bank \
        && dbt run --select analytics --profiles-dir /home/airflow/.dbt \
        && dbt test --select analytics --profiles-dir /home/airflow/.dbt || exit 0
        """
    )
    dbt_run_report_email = EmailOperator(
        task_id="dbt_run_report_email",
        to="jmelvinsarabia032@gmail.com",
        subject="DBT Bank Data-Pipeline Completed",
        html_content="""
        <h3>DBT Bank Data-Pipeline Completed Execution Report</h3>
        <p>The DBT Bank Data-Pipeline Completed has completed successfully.</p>
        <ul>
         """
    )

    dbt_staging >> dbt_snapshot >> dbt_run_analytics >> dbt_run_report_email