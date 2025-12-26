import urllib.parse
from sqlalchemy import create_engine
from scripts.connection.aws import AWSClient
import logging

class PostgresConnection:
    def __init__(self,):
        self.aws = AWSClient()

    def website_forms_engine(self):
        try:
            host = self.aws.get_secret("/coretelecoms/db/host")
            port = self.aws.get_secret("/coretelecoms/db/port")
            name = self.aws.get_secret("/coretelecoms/db/name")
            user = self.aws.get_secret("/coretelecoms/db/user")
            password = self.aws.get_secret("/coretelecoms/db/password")
            
            # URL encode password to handle if password contains '@', '/', etc.)
            encoded_pass = urllib.parse.quote_plus(password)

            connection_str = f"postgresql+psycopg2://{user}:{encoded_pass}@{host}:{port}/{name}"
            
            return create_engine(connection_str)
            
        except Exception as e:
            logging.info(f"❌ Error creating DB Engine: {e}")
            raise ConnectionRefusedError(f"Could not connect to Website form DB, {e}")
