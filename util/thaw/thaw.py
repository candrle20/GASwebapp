# thaw.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import time
import boto3
import json
import time

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'thaw_config.ini'))

def glacier_thaw(job_id, archive_id, filename):
    
    s3 = boto3.client('s3', region_name=config['aws']['AwsRegionName'])
    glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])
    sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])

    encryption = config['aws']['S3_Encryption']
    acl = config['aws']['S3_ACL']
    s3_folder = config['s3']['FolderPrefix']

    try:
        response = glacier.describe_job(
            vaultName=config['glacier']['VaultName'],
            jobId=job_id
        )
        print(f'Job Update Message: {response}')
        
        if response['Completed']:
            file = glacier.get_job_output(
                vaultName=config['glacier']['VaultName'],
                jobId=job_id
            )
            file_body = file['body'].read() 

            try:
                response = s3.put_object(
                    Bucket=config['s3']['ResultsBucket'], 
                    Key=filename,
                    Body=file_body,
                    ServerSideEncryption=encryption,
                    ACL=acl
                    )
                print('Uploaded restored file to S3')
            except Exception as e:
                print(f"Unable to upload file to S3: {e}")
                return False
           
            try:
                glacier.delete_archive(
                    vaultName=config['glacier']['VaultName'],
                    archiveId=archive_id
                )
                print(f'Deleted Glacier Archive {archive_id}')
            except Exception as e:
                print(f'Error deleting archive: {e}')
                return False
        else:
            return False
    except Exception as e:
        print(f'Error restoring job {job_id}: {e}')
        return False

    return True


def SQS_message_reciever(sqs_url):
    sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])
    print(f'SQS Message Reciever Listening on {sqs_url}')

    while True:
        #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html
        sqs_response = sqs.receive_message(
            QueueUrl=sqs_url, 
            MaxNumberOfMessages=10,
            AttributeNames=['All'],
            WaitTimeSeconds=20
            )
        
        if 'Messages' in sqs_response:
            for msg in sqs_response['Messages']:
                msg_body = json.loads(msg['Body'])
                print(f"Received message: {msg_body}")

                message_id = msg_body['MessageId']
                data = json.loads(msg_body['Message'])
                job_id = data['job_id']
                archive_id = data['archive_id']
                filename = data['filename']

                success = glacier_thaw(job_id, archive_id, filename)
                if success:
                    receipt = msg['ReceiptHandle']
                    sqs.delete_message(
                        QueueUrl=sqs_url,
                        ReceiptHandle=receipt
                    )
                    print(f'Message {message_id} Deleted from Queue')
                    print(f'File Restored and Uploaded to S3: {job_id}')
                    
        time.sleep(60)


if __name__ == "__main__":
    sqs_url = config['sqs']['ThawUrl']
    SQS_message_reciever(sqs_url)
### EOF