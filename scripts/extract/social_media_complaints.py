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
            df = pd.read_json(io.BytesIO(json_obj['Body'].read()))

            # add metadata
            df['_data_load_time'] = datetime.now()
            df['_source_file'] = os.path.basename(file_key)

            # write data to parquet
            file_name = os.path.basename(file_key).replace('.json', '.parquet')
            DESTINATION_KEY = f"{DESTINATION_FOLDER}{file_name}"
            
            logging.info(f"-> Writing to {DESTINATION_KEY}")
            out_buffer = io.BytesIO()
            df.to_parquet(out_buffer, index=False)

            #pushing to datalake 
            logging.info(f"Pushing to {DESTINATION_DATA_LAKE}")
            destination_s3_client.put_object(
                Bucket=DESTINATION_DATA_LAKE,
                Key=DESTINATION_KEY,
                Body=out_buffer.getvalue()
            )

            new_files_count += 1
        
        logging.info(f"All {new_files_count} social media complaints data ingested successfully!")

    except Exception as e:
        logging.info(f"Could not get social media complaints data, {e}")
        raise RuntimeError(f"Pipeline Halt: Unable to ingest social media complaints data, {e}")

if __name__ == "__main__":
    extract_social_media_complaint()
