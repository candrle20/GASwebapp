# restore.py
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
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'restore_config.ini'))

def glacier_restore(archive_id):
    glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])

    try:
        #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/initiate_job.html
        response = glacier.initiate_job(
            vaultName=config['glacier']['VaultName'],
            jobParameters={
                'Type': 'archive-retrieval',
                'ArchiveId': archive_id,
                'Tier': 'Expedited'
            }
        )
        job_id = response['jobId']
    except Exception as e:
        print(f'Expedited restore failed {e}, trying standard restore')
        try:
            response = glacier.initiate_job(
                vaultName=config['glacier']['VaultName'],
                jobParameters={
                    'Type': 'archive-retrieval',
                    'ArchiveId': archive_id,
                    'Tier': 'Standard'
                }
            )
            job_id = response['jobId']
        except Exception as e:
            print(f'Standard glacier restore failed {e}')
            return None, False

    return job_id, True

def SQS_message_reciever(sqs_url):
    sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])
    sns = boto3.client('sns', region_name=config['aws']['AwsRegionName'])

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
                archive_id = data['results_file_archive_id']
                user_id = data['user_id']
                filename = data['s3_key_result_file']

                job_id, success = glacier_restore(archive_id)

                if success:
                    receipt = msg['ReceiptHandle']
                    sqs.delete_message(
                        QueueUrl=sqs_url,
                        ReceiptHandle=receipt
                    )
                    print(f'Message {message_id} Deleted from Queue')
                    print(f'Restoring file from glacier- job: {job_id}')
                    
                    #Publish job_id to Thaw SNS topic so thaw can check for completion
                    data = {
                        'job_id': job_id,
                        'user_id': user_id,
                        'archive_id': archive_id,
                        'filename': filename
                    }
                    sns_message = json.dumps({'default': json.dumps(data)})
                    try:
                        sns_topic_response = sns.publish(
                        TopicArn=config['sns']['ThawARN'],
                        Message=sns_message,
                        MessageStructure='json'
                    )
                        print('Published thaw message')
                    except Exception as e:
                        print(f'Error publishing thaw message: {e}')
        time.sleep(10)


if __name__ == "__main__":
    sqs_url = config['sqs']['RestoreUrl']
    SQS_message_reciever(sqs_url)
    
### EOF