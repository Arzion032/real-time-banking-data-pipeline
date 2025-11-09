import os, logging, boto3, snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta, date
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# -------- MinIO Config --------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = os.getenv("MINIO_BUCKET")
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")

# -------- Snowflake Config --------
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

TABLES = ["customers", "accounts", "transactions", "ledger_entries"]

# -------- Python Callables --------
def download_from_minio():
    # Get today's date
    today_str = date.today().strftime("%Y-%m-%d")
    
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    local_files = {}
    for table in TABLES:
        prefix = f"{table}/"
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        objects = resp.get("Contents", [])
        local_files[table] = []

        for obj in objects:
            key = obj["Key"]
            # Extract the date from key: sample key string., transactions/date=2025-11-05/...
            parts = key.split('/')
            if len(parts) >= 2 and parts[1].startswith("date="):
                file_date = parts[1].split('=')[1]
                if file_date == today_str:
                    local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
                    s3.download_file(BUCKET, key, local_file)
                    print(f"Downloaded {key} -> {local_file}")
                    local_files[table].append(local_file)
                    
    return local_files

def load_to_snowflake(**kwargs):
    local_files = kwargs["ti"].xcom_pull(task_ids="download_minio")
    if not local_files:
        print("No files found in MinIO.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()
    files_created = {}
    for table, files in local_files.items():
        i=0
        if not files:
            print(f"No files for {table}, skipping.")
            continue

        for f in files:
            i+=1
            cur.execute(f"PUT file://{f} @%{table}")
            print(f"Uploaded {f} -> @{table} stage")

        copy_sql = f"""
        COPY INTO {table}
        FROM @%{table}
        FILE_FORMAT=(TYPE=PARQUET)
        ON_ERROR='CONTINUE'
        """
        cur.execute(copy_sql)
        print(f"Data loaded into {table}")
        files_created[table] = i
    print("Files loaded into Snowflake:", files_created)
    cur.close()
    conn.close()
    return files_created

def email_report(**kwargs):
    log = logging.getLogger("airflow.task")
    
    # Logging of the task
    log.info("✅ Email report task started")

    # --- Pull XCom from previous dag to get the deets of files loaded---
    ti = kwargs.get("ti")
    files_loaded = ti.xcom_pull(task_ids="load_snowflake") if ti else None
    log.info(f"Pulled XCom from 'load_snowflake': {files_loaded}")

    # --- Email content ---
    if not files_loaded:
        email_content = "No files were loaded into Snowflake today."
        log.warning("No files were loaded into Snowflake today.")
    else:
        email_content = "Files loaded into Snowflake today:\n"
        for table, count in files_loaded.items():
            line = f"- {table}: {count} files\n"
            email_content += line
            log.debug(f"Added line to email: {line.strip()}")

    log.info("✅ Email content generated successfully")

    # --- Send email ---
    try:
        send_email(
            to="jmelvinsarabia032@gmail.com",
            subject="Daily Snowflake Load Report",
            html_content=email_content,
        )
        log.info("📨 Email sent successfully to jmelvinsarabia032@gmail.com")
    except Exception as e:
        log.exception(f"❌ Failed to send email: {e}")
        raise

    log.info("🎯 email_report task completed")
    


# -------- Airflow DAG --------
default_args = {
    "owner": "melvin",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="MinIO_S3_to_Snowflake",
    default_args=default_args,
    description="Load MinIO parquet into Snowflake RAW tables",
    schedule_interval="@daily",
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=["minIO", "scd2", "snowflake"],

) as dag:
    # -------- Tasks --------
    # 1. Download from MinIO
    download_minio = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )
    # 2. Load to Snowflake Staging
    load_snowflake = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True,
    )
    # 3. Email Report
    snowflake_loading_report_email = PythonOperator(
        task_id="snowflake_loading_report_email",
        python_callable=email_report,
        provide_context=True
    )
    # 4. Trigger SCD2 Snapshots DAG
    trigger_dbt_dag = TriggerDagRunOperator(
    task_id="trigger_dbt_dag",
    trigger_dag_id="DBT_Bank_Data_Transformation_Pipeline",
    poke_interval=60,
    )

    download_minio >> load_snowflake >> snowflake_loading_report_email >> trigger_dbt_dag