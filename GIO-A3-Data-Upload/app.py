import uuid
from pprint import pprint
import json
import urllib.parse
import boto3
import os
import csv
import time
import datetime
import uuid

def lambda_handler(event, context):
    # Get the service client.
    s3 = boto3.client('s3')

    print("Event Received: ", event)

    # Extract filename from HTTP Post
    upload_key = event['body']
    upload_key = upload_key[1:-1]
    FileType = os.path.splitext(upload_key)[1][1:]
    FileType = FileType.rstrip(FileType[-1])
    now = datetime.datetime.now()
    Timestamp = now.strftime("%m/%d/%Y %H:%M:%S")
    EmailID = event["requestContext"]["authorizer"]["claims"]["email"]
    UserID = event["requestContext"]["authorizer"]["claims"]["aud"]
    documentid = str(uuid.uuid1())
    print("uuid is: ", documentid)
    print("User: ", EmailID, " Uploading File: ", upload_key, " at ", Timestamp)
    
    # Generate the presigned URL for put requests
    presigned_url = s3.generate_presigned_url(
        ClientMethod='put_object',
        Params={
            'Bucket': os.environ.get('S3_RAW_BUCKET'),
            'Key': documentid + '/' +upload_key,
            'ContentType' : 'text/csv'
        }
    )
    print("Generated Presigned URL: ", presigned_url)
    
    print("Generated Presigned URL: ", presigned_url)
    
    #Making Metadata Entry to DynamoDB
    dynamodb = boto3.resource('dynamodb', endpoint_url="https://dynamodb.us-east-1.amazonaws.com")
    table = dynamodb.Table('dev-gio-dynamodb-02-Gioa3devdb-1SKAAV4N059J3')
    response = table.put_item(
      Item={
            'pk': 'S3FILE',
            'sk': 'S3FILE#s3://' + os.environ.get('S3_RAW_BUCKET') + '/' + documentid + '/' +upload_key,
            'upload_user_adfs_id': UserID,
            'upload_email': EmailID,
            'allowed_users': "user1",
            'allowed_user_groups': 'groupA',
            'file_created_at': Timestamp,
            'status': 'Upload Initiated',
            'documentid': documentid,
            'groupid_docs': 'a1b2c3'
            }
    )

    #Writing Log File to S3 Bucket
    LogString = EmailID + ", " + upload_key + ", " + Timestamp + ", " + FileType
    TimestampFilter = filter(str.isdigit, Timestamp)
    TimestampInteger = "".join(TimestampFilter)
    bucket_name = "gio-a3-data-upload-files-logs"
    EmailNameSplit = EmailID.split("@")
    EmailName = EmailNameSplit[0]
    EmailName = ''.join(filter(str.isalnum, EmailName)) 
    file_name = TimestampInteger + EmailName + ".txt"
    lambda_path = "/tmp/" + file_name
    s3_path = "logs/" + file_name
    os.system('echo ' + LogString + ' > ' + lambda_path)
    s3 = boto3.resource("s3")
    s3.meta.client.upload_file(lambda_path, bucket_name, file_name)

    #Return Presigned URL to Client
    response = {"statusCode": 200, 'headers': {'Access-Control-Allow-Headers': 'Content-Type','Access-Control-Allow-Origin': '*','Access-Control-Allow-Methods': 'OPTIONS,POST'}, "body": json.dumps({"upload_url" : presigned_url})}
    return response


