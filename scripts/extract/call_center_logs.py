from scripts.connection.aws import AWSClient 
from scripts.common.constant import Constant
import io
import pandas as pd
from datetime import datetime
import os
from scripts.common.util import get_existing_files
import logging

# Let's check the root first to find the folder name
SOURCE_DATA_LAKE = Constant.SOURCE_DATA_LAKE
SOURCE_FOLDER = "call logs/"
DESTINATION_DATA_LAKE = Constant.DESTINATION_DATA_LAKE
DESTINATION_FOLDER = "raw/call_center_logs/"
CHUNK_SIZE = Constant.CHUNK_SIZE


def extract_call_center_logs():
    source_client = AWSClient().get_source_s3_client()
    destination_s3_client = AWSClient().local_s3

    logging.info(f"Starting Incremental Ingestion: Call Center Logs")

    try:
        # GET ALREADY PROCESSED FILES
        logging.info("Checking destination for existing files")
        processed_files = get_existing_files(client = destination_s3_client, 
                                             bucket = DESTINATION_DATA_LAKE,
                                             folder = DESTINATION_FOLDER
                                             )
        logging.info(f"Found {len(processed_files)} files already processed.")


        #get list of all files in the 'call logs' dir
        response = source_client.list_objects_v2(
            Bucket=Constant.SOURCE_DATA_LAKE, 
            Prefix=SOURCE_FOLDER
        )
        
        #loop through all the dictionaries in Contents and get 'Key'
        new_files_count = 0

        for item in response['Contents']:
            file_key = item['Key']
            
            #skip non-csv files
            if not file_key.endswith('.csv'):
                continue

            # Check if we already have this file
            filename = os.path.basename(file_key)
            file_stem = os.path.splitext(filename)[0] #removes the extension

            if file_stem in processed_files:
                #skip file, it has been previously ingested
                continue

            #read csv
            logging.info(f"Reading: {file_key} ...")
            csv_obj = source_client.get_object(Bucket=Constant.SOURCE_DATA_LAKE, Key=file_key)

            #iterate ove the boto csv object in chunks
            csv_chunk = pd.read_csv(io.BytesIO(csv_obj['Body']), chunksize=CHUNK_SIZE) 
            counter = 1

            for call_log_chunk_df in csv_chunk:
                # add metadata to this specific chunk
                call_log_chunk_df['_data_load_time'] = datetime.now()
                call_log_chunk_df['_source_file'] = filename

                # use unique filename for this chunk using the base calllog file name
                chunk_filename = f"{file_stem}_chunk_{counter}.parquet"
                CHUNK_DESTINATION_KEY = f"{DESTINATION_FOLDER}{chunk_filename}"
                
                # Write chunk to buffer
                out_buffer = io.BytesIO()
                call_log_chunk_df.to_parquet(out_buffer, index=False)

                # Push chunk to datalake
                logging.info(f"Writing to {CHUNK_DESTINATION_KEY}")
                destination_s3_client.put_object(
                    Bucket=DESTINATION_DATA_LAKE,
                    Key=CHUNK_DESTINATION_KEY,
                    Body=out_buffer.getvalue()
                )
                
                #prepare for next chunk
                counter += 1

            new_files_count += 1
        
        logging.info(f"All {new_files_count} call logs data ingested successfully!")

    except Exception as e:
        logging.info(f"Could not get call center logs, {e}")
        raise RuntimeError(f"Pipeline Halt: Unable to get call center logs, {e}")

if __name__ == "__main__":
    extract_call_center_logs()
    