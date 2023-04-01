import boto3
import os
import requests
import botocore
import json

# Constants
SECRETS_URL = "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/ds_code_challenge_creds.json"
S3_REGION = "af-south-1"
S3_BUCKET = "cct-ds-code-challenge-input-data"

def get_s3_secrets(url: str):
    response = requests.get(SECRETS_URL).json()
    keys = response['s3']
    return keys['access_key'], keys['secret_key']
    
def init_s3_client(s3_region : str):
    access_key, secret_key = get_s3_secrets(SECRETS_URL)
    s3_client = boto3.client("s3",
                            region_name = s3_region
                            ,aws_access_key_id = access_key
                            ,aws_secret_access_key = secret_key
                            )
    return s3_client

def get_s3_object_list(s3_client: botocore.client, s3_bucket: str):
    s3_bucket_objects = s3_client.list_objects(Bucket = s3_bucket)
    files = s3_bucket_objects['Contents']
    objects = [file['Key'] for file in files]
    return objects

def download_s3_object(s3_client: botocore.client, s3_bucket: str, filename: str):
    objects = get_s3_object_list(s3_client, s3_bucket)
    filename = 'city-hex-polygons-8.geojson'
    fileExists = True if filename in objects else False
    download_complete = False
    if os.path.exists('./city-hex-polygons-8.geojson'):
        download_complete = True
    else:
        if fileExists:
            s3_client.download_file(s3_bucket, 
                                    filename, 
                                    filename
                                    )
        download_complete = True
        
    return download_complete

def s3_select_query(s3_client: botocore.client, s3_bucket: str, object: str, resolution: int):
    response = s3_client.select_object_content(
            Bucket = s3_bucket,
            Key = object,
            ExpressionType = 'SQL',
            Expression = f"SELECT * from S3Object[*].features[*] rec where rec.properties.resolution = {resolution}", 
            InputSerialization = {"JSON": {"Type": "DOCUMENT"}, "CompressionType": "NONE"},
            OutputSerialization = {'JSON': {}}
        )
    
    return response['Payload']

def s3_stream_to_dict(query_result: botocore.eventstream):
    parsed_response = ""
    for event in query_result:
        if 'Records' in event:
            res = event['Records']['Payload'].decode('utf-8')
            parsed_response += res
    response = [json.loads(line) for line in parsed_response.split()]

    return response
