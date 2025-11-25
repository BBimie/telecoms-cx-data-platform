from scripts.connection.aws import AWSClient 
from scripts.common.constant import Constant
import io
import pandas as pd
from datetime import datetime
import os

# Let's check the root first to find the folder name
SOURCE_DATA_LAKE = Constant.SOURCE_DATA_LAKE
SOURCE_FOLDER = "call logs/"
DESTINATION_DATA_LAKE = Constant.DESTINATION_DATA_LAKE
DESTINATION_FOLDER = "raw/call_center_logs/"


def get_existing_files(client, bucket, folder):
    """
    Returns a set of filenames (without extension) that already exist in the destination bucket to prevent duplicating log fetching.
    """
    existing_files = set()
    paginator = client.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket, Prefix=folder):
        if 'Contents' in page:
            for obj in page['Contents']:
                # filename "log_2025.parquet" from "raw/call_center/"
                filename = os.path.basename(obj['Key'])
                # Remove extension to get the actual file name "log_2025"
                file_stem = os.path.splitext(filename)[0]
                existing_files.add(file_stem)
                
    return existing_files


def extract_call_center_logs():
    source_client = AWSClient().get_source_s3_client()
    destination_s3_client = AWSClient().local_s3()

    print(f"Starting Incremental Ingestion: Call Center Logs")

    try:
        # GET ALREADY PROCESSED FILES
        print("Checking destination for existing files")
        processed_files = get_existing_files(client = destination_s3_client, 
                                             bucket = DESTINATION_DATA_LAKE,
                                             folder = DESTINATION_FOLDER
                                             )
        print(f"Found {len(processed_files)} files already processed.")


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
            file_stem = os.path.splitext(filename)[0]

            if file_stem in processed_files:
                #skip file, it has been previously ingested
                continue

            #read csv
            print(f"Reading: {file_key} ...")
            csv_obj = source_client.get_object(Bucket=Constant.SOURCE_DATA_LAKE, Key=file_key)
            df = pd.read_csv(io.BytesIO(csv_obj['Body'].read()))

            # add metadata
            df['_data_load_time'] = datetime.now()
            df['_source_file'] = os.path.basename(file_key)

            # write data to parquet
            file_name = os.path.basename(file_key).replace('.csv', '.parquet')
            DESTINATION_KEY = f"{DESTINATION_FOLDER}{file_name}"
            
            print(f"-> Writing to {DESTINATION_KEY}")
            out_buffer = io.BytesIO()
            df.to_parquet(out_buffer, index=False)

            #pushing to datalake 
            print(f"Pushing to {DESTINATION_DATA_LAKE}")
            AWSClient().local_s3.put_object(
                Bucket=DESTINATION_DATA_LAKE,
                Key=DESTINATION_KEY,
                Body=out_buffer.getvalue()
            )

            new_files_count += 1
        
        print(f"All {new_files_count} call logs data ingested successfully!")

    except Exception as e:
        print(f"Could not get call center logs, {e}")


# def delete_folder():
#     s3 = AWSClient().local_s3

#     print(f"DELETING all files in: s3://{DESTINATION_DATA_LAKE}/{DESTINATION_FOLDER}")

#     # 1. List all objects in the folder
#     # S3 doesn't have "folders", so we find everything starting with the prefix
#     paginator = s3.get_paginator('list_objects_v2')
#     pages = paginator.paginate(Bucket=DESTINATION_DATA_LAKE, Prefix=DESTINATION_FOLDER)

#     deleted_count = 0

#     for page in pages:
#         if 'Contents' in page:
#             objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
            
#             # 2. Delete in batches (Efficient)
#             if objects_to_delete:
#                 print(f"   ...Deleting batch of {len(objects_to_delete)} files...")
#                 s3.delete_objects(
#                     Bucket=DESTINATION_DATA_LAKE,
#                     Delete={'Objects': objects_to_delete}
#                 )
#                 deleted_count += len(objects_to_delete)

#     if deleted_count > 0:
#         print(f"✅ Successfully deleted {deleted_count} files.")
#     else:
#         print("Folder was already empty.")