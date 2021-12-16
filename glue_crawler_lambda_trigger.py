import json
import boto3
import sys

client = boto3.client('glue')

def lambda_handler(event, context):
    print(event)
    
    folder_name = event['Records'][0]['s3']['object']['key'].split("/")[0]
    print("folder_name in S3: "+folder_name)
    if folder_name == "movies":
        client.start_crawler(Name='midterm_csv_crawler')
    else:
        client.start_crawler(Name='midterm_json_crawler')