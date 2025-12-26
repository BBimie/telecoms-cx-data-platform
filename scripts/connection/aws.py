import boto3
from scripts.common.constant import Constant
import logging


class AWSClient:
    def __init__(self, region="eu-north-1"):
        self.region = region
        self.ssm = boto3.client('ssm', region_name=self.region)
        self.local_s3 = boto3.client('s3', region_name=self.region)
        
        self._source_s3_client = None

    def get_secret(self, param_name):
        """Fetches a SecureString from the SSM Parameter Store."""
        try:
            response = self.ssm.get_parameter(
                Name=param_name, 
                WithDecryption=True
            )
            return response['Parameter']['Value']
        except Exception as e:
            logging.info(f"Error fetching parameter {param_name}: {e}")
            raise ResourceWarning(f"Could not connect to AWS SMM, {e}")

    def get_source_s3_client(self):
        """
        Returns a boto3 client authenticated with the DataSource Account keys.
        """
        if self._source_s3_client:
            return self._source_s3_client

        logging.info("Authenticating with Source Account...")
        access_key = Constant.SOURCE_AWS_ACCESS_KEY_ID
        secret_key = Constant.SOURCE_AWS_SECRET_ACCESS_KEY
        try:
            self._source_s3_client = boto3.client(
                's3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=self.region
            )
            return self._source_s3_client
        
        except Exception as e:
            logging.info(f"Error authenticating with Source Account: {e}")
            raise ResourceWarning(f"Could not connect to Source AWS S3, {e}")