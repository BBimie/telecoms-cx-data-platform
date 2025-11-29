import io
import pandas as pd
from datetime import datetime
from scripts.connection.aws import AWSClient
from scripts.common.constant import Constant

#configs
CUSTOMER_DATA_SOURCE_KEY = "customers/customers_dataset.csv"
DESTINATION_KEY = "raw/customers/customers.parquet"

def extract_customer_data():
    aws = AWSClient()
    source_client = aws.get_source_s3_client()
    try:
        print(f"Reading from Customer data from source, {Constant.SOURCE_DATA_LAKE} ")
        obj = source_client.get_object(Bucket=Constant.SOURCE_DATA_LAKE, 
                                       Key=CUSTOMER_DATA_SOURCE_KEY)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))

        print(f"Customer data columns: {df.columns}")
        print(f"Customer data shape: {df.shape}")
        print(f"Customer data head: {df.head()}")

        #adding load time metadata tracking 
        df['_data_load_time'] = datetime.now()
        
        #writing to to parquet
        print("Writing to customer data to Parquet")
        out_buffer = io.BytesIO()
        df.to_parquet(out_buffer, index=False)
        
        aws.local_s3.put_object(
            Bucket=Constant.DESTINATION_DATA_LAKE,
            Key=DESTINATION_KEY,
            Body=out_buffer.getvalue()
        )
        print("Ingestion Complete!")
    except Exception as e:
        print(f"Could not ingest customer data, {e}")

if __name__ == "__main__":
    extract_customer_data()