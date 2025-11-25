import boto3

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
            print(f"Error fetching parameter {param_name}: {e}")
            raise e

    def get_source_s3_client(self):
        """
        Returns a boto3 client authenticated with the DataSource Account keys.
        """
        if self._source_s3_client:
            return self._source_s3_client

        print("Authenticating with Source Account...")
        access_key = self.get_secret('/coretelecoms/source/access_key')
        secret_key = self.get_secret('/coretelecoms/source/secret_key')

        self._source_s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=self.region
        )
        return self._source_s3_client