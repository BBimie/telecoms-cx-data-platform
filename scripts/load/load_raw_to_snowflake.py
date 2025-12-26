from scripts.connection.snowflake import get_snowflake_connection
from scripts.common.constant import Constant
import logging


def copy_data_from_s3_to_snowflake_query(table_name :str, 
                                         s3_folder :str, 
                                         database :str=Constant.CORETELECOMS_DATABASE, 
                                         schema :str  =Constant.CORETELECOMS_BRONZE_SCHEMA, 
                                         stage_name :str=Constant.SNOWFLAKE_STAGE) -> str:
    """
    This function generates a COPY INTO statement that loads data into a 'data' 
    column and appends a 'snowflake_load_time' timestamp. It relies on 
    infrastructure assumptions (IAC) that the target table has these specific 
    columns and that the specific file format exists in the schema.

    Args:
        table_name (str): The name of the target Snowflake table.
        s3_folder (str): The specific S3 subfolder (prefix) to load data from.
        database (str): The target Snowflake database name. Defaults to Constant.
        schema (str): The target Snowflake schema name. Defaults to Constant.
        stage_name (str): The name of the external stage. Defaults to Constant.

    Returns:
        str: The fully formatted executable SQL query string.
    """
    query = f"""
        COPY INTO {database}.{schema}.{table_name} ("data", "snowflake_load_time")
        FROM (
            SELECT $1, CURRENT_TIMESTAMP()
            FROM @{database}.{schema}.{stage_name}/{s3_folder}/
        )
        FILE_FORMAT = (FORMAT_NAME = '{database}.{schema}.PARQUET_FORMAT')
        ON_ERROR = 'CONTINUE'; """

    return query      
    

def copy_data_from_s3_to_snowflake(table_name, s3_folder, database, schema):
    conn = get_snowflake_connection(database=database, schema=schema)
    
    ### THE INDIVIDUAL TABLES have been defined with two columns, variant, and load_time to be able to successfully load all the data with no errors
    # S3_RAW_DATA: The S3 STAGE which is a shortcut that was created on snowflake to connect directly to my s3 bucket (created in terraform)
    # PARQUET_FORMAT here is the saved file format object created in terraform
    # THE column data has the datatype VARIANT which allows it to store ALL FORMS of data types

    with conn.connect() as cursor:

        try:
            logging.info(f"Loading {table_name} from {s3_folder} s3 folder.")
            
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
            
            logging.info(f"Result: {result[0]}")
            
        except Exception as e:
            logging.info(f"Error loading {table_name}: {e}")
            raise ResourceWarning(f"Error loading data into {table_name}: {e}")

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