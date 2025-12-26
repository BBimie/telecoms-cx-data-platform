import sys
from airflow.sdk import DAG
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

sys.path.append('/opt/airflow')
from scripts.common.constant import Constant


#setup datasouce > table name mapping
table_mappings = {
        "CUSTOMERS": "customers",
        "AGENTS": "agents",
        "CALL_CENTER_LOGS": "call_center_logs",
        "WEBSITE_FORMS": "website_form_complaints",
        "SOCIAL_MEDIA": "social_media"
    }

default_args= {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

#empty list to hold all the load tasks
load_tasks_list = [] 

with DAG(
    dag_id="load_data_dag",
    default_args=default_args,
    description='Loads the raw data into the RAW snowflake schema',
    schedule=None,
    start_date=datetime(2025, 11, 27),
    catchup=False,
    tags=['capstone', 'load_layer', 'daily'],

) as dag:
    for table_name, s3_source in table_mappings.items():

        stage_path = f"S3_RAW_DATA/{s3_source}"

        #dynamic tasks to load all the data into snowflake
        task = CopyFromExternalStageToSnowflakeOperator(
            task_id=f"copy_into_snowflake_{table_name}_table",
            snowflake_conn_id="telecoms_conn_id",
            table=f"{Constant.CORETELECOMS_BRONZE_SCHEMA}.{table_name}",
            stage=stage_path,
            pattern=".*[.]parquet",
            file_format=f"{Constant.CORETELECOMS_BRONZE_SCHEMA}.PARQUET_FORMAT",
            )
        
        #append each of the tasks to the task list
        load_tasks_list.append(task)
    
    # This runs only after ALL load tasks complete successfully
    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform_dag",
        trigger_dag_id="transform_data_dag",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    
    load_tasks_list >> trigger_transform