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
import logging

DESTINATION_DATA_LAKE = Constant.DESTINATION_DATA_LAKE
DESTINATION_FOLDER = "raw/website_form_complaints/"
db_engine = PostgresConnection().website_forms_engine()
db_schema = AWSClient().get_secret(param_name='/coretelecoms/db/schema')
CHUNK_SIZE = Constant.CHUNK_SIZE

def table_discovery():
    #sqlalchemy method that returns list of tables in the db
    db_tables = inspect(db_engine).get_table_names(schema=db_schema)
    target_tables = [t for t in db_tables  if t.startswith('web_form_request')]

    logging.info(f"The {db_schema} database has {len(target_tables)} tables containing customer complaints.")

    return target_tables

def extract_web_form_complaints():

    target_tables = table_discovery()

    #checking tables already ingested
    destination_s3_client = AWSClient().local_s3
    processed_files = get_existing_files(client = destination_s3_client, 
                                         bucket = DESTINATION_DATA_LAKE,
                                         folder = DESTINATION_FOLDER
                                        )
    try:
        new_tables_count = 0
        with db_engine.connect() as conn:
            for table_name in target_tables:
                logging.info("Running incremental load of tables. NEW TABLES ONLY")
                if table_name in processed_files:
                    #skip table
                    continue

                query = text(f"SELECT * FROM {db_schema}.{table_name}")
                chunk_df = pd.read_sql(sql=query, con=conn, chunksize=CHUNK_SIZE)

                counter = 1
                has_data = False
                
                #loop thrugh the chunk iterator
                for website_form_chunk_df in chunk_df:
                    has_data = True
                    
                    # add metadata to each chunk
                    website_form_chunk_df['_data_load_time'] = datetime.now()
                    website_form_chunk_df['_source_table'] = table_name

                    #generate file name
                    file_name = f"{table_name}_chunk_{counter}.parquet"
                    CHUNK_DESTINATION_KEY = f"{DESTINATION_FOLDER}{file_name}"
                    
                    logging.info(f"Uploading chunk {counter} to {CHUNK_DESTINATION_KEY}")
                    
                    out_buffer = io.BytesIO()
                    website_form_chunk_df.to_parquet(out_buffer, index=False)

                    destination_s3_client.put_object(
                        Bucket=DESTINATION_DATA_LAKE,
                        Key=CHUNK_DESTINATION_KEY,
                        Body=out_buffer.getvalue()
                    )
                    
                    counter += 1

                if has_data:
                    new_tables_count += 1
            
            logging.info(f"Successfully ingested {new_tables_count} new tables from website form complaints databases")
            
    except Exception as e:
        logging.info(f"Could not get websit form data, {e}")
        raise RuntimeError(f"Pipeline Halt: Unable to ingest website form data, {e}")


if __name__ == "__main__":
    extract_web_form_complaints()