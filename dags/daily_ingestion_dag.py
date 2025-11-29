from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False, 
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'coretelecoms_ingestion_daily_1',
    default_args=default_args,
    description='Ingests Raw data to Data Lake',
    schedule='@daily',
    start_date=datetime(2025, 11, 27),
    catchup=False,
    tags=['capstone', 'ingestion_layer1', 'daily'],
) as dag:

    t3_call_logs = BashOperator(
        task_id='ingest_call_logs',
        bash_command='export PYTHONPATH=/opt/airflow && python /opt/airflow/scripts/extract/call_center_logs.py'
    )

    t4_web_forms = BashOperator(
        task_id='ingest_web_forms',
        bash_command='export PYTHONPATH=/opt/airflow && python /opt/airflow/scripts/extract/website_form_complaints.py'
    )

    t5_social_media = BashOperator(
        task_id='ingest_social_media',
        bash_command='export PYTHONPATH=/opt/airflow && python /opt/airflow/scripts/extract/social_media_complaints.py'
    )

    # Loading INTO SNOWFLAKE
    #RUNS AFTER INGESTION
    t5_load_snowflake = BashOperator(
        task_id='load_raw_to_snowflake',
        bash_command='export PYTHONPATH=/opt/airflow && python /opt/airflow/scripts/load/load_raw_to_snowflake.py'
    )

    [t3_call_logs, t4_web_forms, t5_social_media] >> t5_load_snowflake
