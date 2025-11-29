import os
from snowflake.connector import connect
from scripts.common.constant import Constant

def get_snowflake_connection(database, schema):
    connection = connect(
        user = Constant.SNOWFLAKE_USER,
        password = Constant.SNOWFLAKE_PASSWORD,
        account = Constant.SNOWFLAKE_ACCOUNT,
        warehouse = Constant.SNOWFLAKE_WAREHOUSE,
        database = database,
        schema = schema,
        role = Constant.SNOWFLAKE_ROLE
    )

    return connection