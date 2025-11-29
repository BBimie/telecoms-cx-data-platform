import sys
import os
from scripts.connection.aws import AWSClient
from scripts.common.constant import Constant
from scripts.connection.postgres import PostgresConnection
from sqlalchemy import inspect, text
import pandas as pd
from scripts.common.util import get_existing_files
from datetime import datetime
import io

DESTINATION_DATA_LAKE = Constant.DESTINATION_DATA_LAKE
DESTINATION_FOLDER = "raw/website_form_complaints/"
db_engine = PostgresConnection().website_forms_engine()
db_schema = AWSClient().get_secret(param_name='/coretelecoms/db/schema')

def table_discovery():
    #sqlalchemy method that returns list of tables in the db
    db_tables = inspect(db_engine).get_table_names(schema=db_schema)
    target_tables = [t for t in db_tables  if t.startswith('web_form_request')]

    print(f"The {db_schema} database has {len(target_tables)} tables containing customer complaints.")

    return target_tables

def extract_web_form_complaints():

    target_tables = table_discovery()

    #checking tables already ingested
    destination_s3_client = AWSClient().local_s3
    processed_files = get_existing_files(client = destination_s3_client, 
                                         bucket = DESTINATION_DATA_LAKE,
                                         folder = DESTINATION_FOLDER
                                        )
    new_tables_count = 0
    with db_engine.connect() as conn:
        for table_name in target_tables:
            print("Running incremental load of tables. NEW TABLES ONLY")
            if table_name in processed_files:
                #skip table
                continue

            query = text(f"SELECT * FROM {db_schema}.{table_name}")
            df = pd.read_sql(sql=query, con=conn)

            # add metadata
            df['_data_load_time'] = datetime.now()
            df['_source_table'] = os.path.basename(table_name)


            # write data to parquet
            file_name = os.path.basename(f'{table_name}.parquet')
            DESTINATION_KEY = f"{DESTINATION_FOLDER}{file_name}"
            
            print(f"-> Writing to {DESTINATION_KEY}")
            out_buffer = io.BytesIO()
            df.to_parquet(out_buffer, index=False)

            # #pushing to datalake 
            print(f"Pushing to {DESTINATION_DATA_LAKE}")
            destination_s3_client.put_object(
                Bucket=DESTINATION_DATA_LAKE,
                Key=DESTINATION_KEY,
                Body=out_buffer.getvalue()
            )

            new_tables_count += 1
        
        print(f"Successfully ingested {new_tables_count} new tables from website form complaints databases")


if __name__ == "__main__":
    extract_web_form_complaints()