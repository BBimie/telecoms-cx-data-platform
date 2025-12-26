from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args= {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="transform_data_dag",
    default_args=default_args,
    description='Transforms the data through the 3 layers',
    schedule=None,
    start_date=datetime(2025, 11, 27),
    catchup=False,
    tags=['capstone', 'tranform_layer', 'daily'],

) as dag:

    # transform (dbt)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt/dbt_core_telecoms && dbt run --profiles-dir .'
    )

    #runs the schema.yml
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt/dbt_core_telecoms && dbt test --profiles-dir .'
    )

    dbt_run >> dbt_test