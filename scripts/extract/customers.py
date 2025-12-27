import io
import pandas as pd
from datetime import datetime
from scripts.connection.aws import AWSClient
from scripts.common.constant import Constant
import logging

#configs
CUSTOMER_DATA_SOURCE_KEY = "customers/customers_dataset.csv"
DESTINATION_FOLDER = "raw/customers/"
DESTINATION_DATA_LAKE=Constant.DESTINATION_DATA_LAKE
CHUNK_SIZE=Constant.CHUNK_SIZE
destination_s3_client=AWSClient().local_s3

def extract_customer_data():
    aws = AWSClient()
    source_client = aws.get_source_s3_client()
    try:
        logging.info(f"Reading from Customer data from source, {Constant.SOURCE_DATA_LAKE} ")
        csv_obj = source_client.get_object(Bucket=Constant.SOURCE_DATA_LAKE, 
                                       Key=CUSTOMER_DATA_SOURCE_KEY)
        
        #iterate ove the boto csv object in chunks
        csv_chunk = pd.read_csv(io.BytesIO(csv_obj['Body']), chunksize=CHUNK_SIZE) 
        counter = 1

        for customer_chunk_df in csv_chunk:

            #run customer data overview once
            if counter == 1:
                logging.info(f"Customer chunk data columns: {customer_chunk_df.columns}")

            #adding load time metadata tracking to this chunk
            customer_chunk_df['_data_load_time'] = datetime.now()

            # use unique filename for this chunk using the base calllog file name
            chunk_filename = f"customer_chunk_{counter}.parquet"
            CHUNK_DESTINATION_KEY = f"{DESTINATION_FOLDER}{chunk_filename}"
            
            # Write chunk to buffer
            out_buffer = io.BytesIO()
            customer_chunk_df.to_parquet(out_buffer, index=False)

            # Push chunk to datalake
            logging.info(f"Writing to {CHUNK_DESTINATION_KEY}")
            destination_s3_client.put_object(
                Bucket=DESTINATION_DATA_LAKE,
                Key=CHUNK_DESTINATION_KEY,
                Body=out_buffer.getvalue()
            )

            #prepare for next chunk
            counter += 1

        logging.info("Ingestion Complete!")
        
    except Exception as e:
        logging.info(f"Could not ingest customer data, {e}")
        raise RuntimeError(f"Pipeline Halt: Unable to ingest customer data, {e}")

if __name__ == "__main__":
    extract_customer_data()