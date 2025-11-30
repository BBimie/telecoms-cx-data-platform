from scripts.connection.snowflake import get_snowflake_connection
from scripts.common.constant import Constant

def copy_data_from_s3_to_snowflake(table_name, s3_folder, database, schema):
    conn = get_snowflake_connection(database=database, schema=schema)
    cursor = conn.cursor()
    
    ### THE INDIVIDUAL TABLES have been defined with two columns, variant, and load_time to be able to successfully load all the data with no errors
    # S3_RAW_DATA: The S3 STAGE which is a shortcut that was created on snowflake to connect directly to my s3 bucket (created in terraform)
    # PARQUET_FORMAT here is the saved file format object created in terraform
    # THE column data has the datatype VARIANT which allows it to store ALL FORMS of data types

    try:
        print(f"Loading {table_name} from {s3_folder} s3 folder.")
        
        # this query is basically saying, load into the table using the source s3 bucket, and open the parquet file, 
        # and load each row of the parquet into the data column, and then store the snowflake load time
        # the pipeline will never break due to data schema changes
        query = f"""
            COPY INTO {database}.{schema}.{table_name} ("data", "snowflake_load_time")
            FROM (
                SELECT $1, CURRENT_TIMESTAMP()
                FROM @{database}.{schema}.S3_RAW_DATA/{s3_folder}/
            )
            FILE_FORMAT = (FORMAT_NAME = '{database}.{schema}.PARQUET_FORMAT')
            ON_ERROR = 'CONTINUE'; 
        """
        
        cursor.execute(query)
        result = cursor.fetchall()
        
        print(f"Result: {result[0]}")
        
    except Exception as e:
        print(f"Error loading {table_name}: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # mapping Snowflake Table to S3 Folder
    mappings = {
        "CUSTOMERS": "customers",
        "AGENTS": "agents",
        "CALL_CENTER_LOGS": "call_center_logs",
        "WEBSITE_FORMS": "website_form_complaints",
        "SOCIAL_MEDIA": "social_media"
    }
    
    for table, folder in mappings.items():
        copy_data_from_s3_to_snowflake(table, 
                                       folder, 
                                       database=Constant.CORETELECOMS_DATABASE, 
                                       schema='RAW')