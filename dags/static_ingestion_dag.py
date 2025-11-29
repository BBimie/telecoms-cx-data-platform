from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
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
    t1_customers = BashOperator(
        task_id='ingest_customers',
        bash_command='export PYTHONPATH=/opt/airflow && python /opt/airflow/scripts/extract/customers.py'
    )

    t2_agents = BashOperator(
        task_id='ingest_agents',
        bash_command='export PYTHONPATH=/opt/airflow && python /opt/airflow/scripts/extract/agents.py'
    )


    [t1_customers, t2_agents]
