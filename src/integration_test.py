# This script is used to test the integration to the AWS Client and S3 resource

import boto3
import botocore
import requests
from loguru import logger

# Constants
SECRETS_URL = "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/ds_code_challenge_creds.json"
S3_REGION = "af-south-1"
S3_BUCKET = "cct-ds-code-challenge-input-data"

# Test integration to create client using provided credentials
def init_s3_client(s3_region : str):
    response = requests.get(SECRETS_URL).json()
    keys = response['s3']
    access_key, secret_key = keys['access_key'], keys['secret_key']
    s3_client = boto3.client("s3",
                            region_name = s3_region
                            ,aws_access_key_id = access_key
                            ,aws_secret_access_key = secret_key
                            )
    assert (s3_client.__class__.__name__ == "S3")

# Test integration to AWS using supplied credentials
def get_s3_secrets():
    response = requests.get(SECRETS_URL).json()
    keys = response['s3']
    assert(isinstance(keys['access_key'], str))
    assert(isinstance(keys['secret_key'], str))

# Test integration to access files
def get_s3_object_list(s3_client: botocore.client):
    s3_bucket_objects = s3_client.list_objects(Bucket = S3_BUCKET)
    files = s3_bucket_objects['Contents']
    objects = [file['Key'] for file in files]
    assert all(isinstance(s, str) for s in objects)

# Function to run all tests
def integration_tests():
    logger.info("Initiating S3 Client")
    response = requests.get(SECRETS_URL).json()
    keys = response['s3']
    access_key, secret_key = keys['access_key'], keys['secret_key']
    s3_client = boto3.client("s3",
                            region_name = S3_REGION
                            ,aws_access_key_id = access_key
                            ,aws_secret_access_key = secret_key
                            )
    
    logger.info("Testing access to Retrieving Access Keys and Access Secrets")
    get_s3_secrets()
    logger.success("get_s3_secrets integration test succeeded")

    logger.info("Test integration to AWS using supplied credentials/secrets")
    init_s3_client(S3_REGION)
    logger.success("init_s3_client integration test succeeded")
    
    logger.info("Test integration to AWS S3 to retrieve challenge data and validation data")
    get_s3_object_list(s3_client)
    logger.success("get_s3_object_list integration test succeeded")

integration_tests()