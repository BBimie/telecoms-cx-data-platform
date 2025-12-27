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
SOURCE_FOLDER = "social_medias/"
DESTINATION_DATA_LAKE = Constant.DESTINATION_DATA_LAKE
DESTINATION_FOLDER = "raw/social_media/"
CHUNK_SIZE=Constant.CHUNK_SIZE


def extract_social_media_complaint():
    source_client = AWSClient().get_source_s3_client()
    destination_s3_client = AWSClient().local_s3

    logging.info(f"Starting Incremental Ingestion: Social Media Complaints")

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
        logging.info(response)
        
        #loop through all the dictionaries in Contents and get 'Key'
        new_files_count = 0

        for item in response['Contents']:
            file_key = item['Key']
            
            #skip non-json files
            if not file_key.endswith('.json'):
                continue

            # Check if we already have this file
            filename = os.path.basename(file_key)
            file_stem = os.path.splitext(filename)[0]

            if file_stem in processed_files:
                #skip file, it has been previously ingested
                continue

            #read json
            logging.info(f"Reading: {file_key}")
            json_obj = source_client.get_object(Bucket=Constant.SOURCE_DATA_LAKE, Key=file_key)

            #start chunk
            json_chunk = pd.read_json(json_obj['Body'], lines=True, chunksize=CHUNK_SIZE)
            counter = 1

            for social_media_chunk_df in json_chunk:
                # metadata
                social_media_chunk_df['_data_load_time'] = datetime.now()
                social_media_chunk_df['_source_file'] = filename

                #generate parquet filename
                chunk_filename = f"{file_stem}_chunk_{counter}.parquet"
                DESTINATION_KEY = f"{DESTINATION_FOLDER}{chunk_filename}"
                
                #save data to parquet
                out_buffer = io.BytesIO()
                social_media_chunk_df.to_parquet(out_buffer, index=False)

                # push to s3
                logging.info(f"Uploading chunk {chunk_counter} to {DESTINATION_KEY}")
                destination_s3_client.put_object(
                    Bucket=DESTINATION_DATA_LAKE,
                    Key=DESTINATION_KEY,
                    Body=out_buffer.getvalue()
                )
                
                counter += 1

            new_files_count += 1
        
        logging.info(f"All {new_files_count} social media complaints data ingested successfully!")

    except Exception as e:
        logging.info(f"Could not get social media complaints data, {e}")
        raise RuntimeError(f"Pipeline Halt: Unable to ingest social media complaints data, {e}")

if __name__ == "__main__":
    extract_social_media_complaint()
