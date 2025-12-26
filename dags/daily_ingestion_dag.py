import sys
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

sys.path.append('/opt/airflow')
from scripts.extract.call_center_logs import extract_call_center_logs
from scripts.extract.social_media_complaints import extract_social_media_complaint
from scripts.extract.website_form_complaints import extract_web_form_complaints

from scripts.common.constant import Constant

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='coretelecoms_ingestion_daily_1',
    default_args=default_args,
    description='Ingests Raw data to Data Lake daily',
    schedule='@daily',
    start_date=datetime(2025, 11, 27),
    catchup=False,
    tags=['capstone', 'ingestion_layer1', 'daily'],
) as dag:
    
    #EXTRACTION DAGS
    t3_call_logs = PythonOperator(
        task_id='ingest_call_logs',
        python_callable=extract_call_center_logs
    )

    t4_web_forms = PythonOperator(
        task_id='ingest_web_forms',
        python_callable=extract_web_form_complaints
    )

    t5_social_media = PythonOperator(
        task_id='ingest_social_media',
        python_callable=extract_social_media_complaint
    )

    trigger_load = TriggerDagRunOperator(
        task_id="trigger_load_dag",
        trigger_dag_id="load_data_dag",
        wait_for_completion=False
    )


    [t3_call_logs, t4_web_forms, t5_social_media] >> trigger_load
