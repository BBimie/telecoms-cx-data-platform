from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow')
from scripts.extract.customers import extract_customer_data
from scripts.extract.agents import extract_agents_data

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay' : timedelta(minutes=5),
}

#runs the static customer and agent data ingestion once
with DAG(
    'coretelecoms_ingestion_static_1',
    default_args=default_args,
    description='Ingests Raw data to Data Lake',
    schedule='@once',
    start_date=datetime(2025, 11, 27),
    catchup=False,
    tags=['capstone', 'ingestion_layer1', 'static'],
) as dag:
    
    # The usage remains exactly the same, only the import changed
    t1_customers = PythonOperator(
        task_id='ingest_customers',
        python_callable=extract_customer_data,
    )

    t2_agents = PythonOperator(
        task_id='ingest_agents',
        python_callable=extract_agents_data,
    )


    [t1_customers, t2_agents]
