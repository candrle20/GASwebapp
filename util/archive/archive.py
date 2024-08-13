# archive.py
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
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'archive_config.ini'))

def glacierArchive(job_data):
    
    s3 = boto3.client('s3', region_name=config['aws']['AwsRegionName'])
    glacier = boto3.client('glacier', region_name=config['aws']['AwsRegionName'])
    dynamodb = boto3.client('dynamodb', region_name=config['aws']['AwsRegionName'])
    glacier_vault = config['glacier']['VaultName']
    bucket = config['s3']['ResultsBucket']
    result_filename = job_data['s3_key_result_file']

    try:
        result_file = s3.get_object(Bucket=bucket, 
                                    Key=result_filename
                                    )
        file_data = result_file['Body'].read()

    except Exception as e:
        print(f'Error error getting file data: {e}')
        return False
    
    try:
        #Upload to Glacier
        #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/upload_archive.html
        response = glacier.upload_archive(
                                            vaultName=glacier_vault,
                                            body=file_data
                                        )
        archive_id = response['archiveId']
        print('Glacier upload complete')
    except Exception as e:
        print(f'Error uploading to glacier: {e}')
        return False
    
    try:
        #Update DynamoDB
        dynamodb.update_item(
            TableName=config['dynamo']['Table'],
            Key={'job_id': {'S': job_data['job_id']}},
            UpdateExpression="set results_file_archive_id = :a",
            ExpressionAttributeValues={':a': {'S': archive_id}}
        )
        print('Updated table')
    except Exception as e:
        print(f'Error updating dynamoDB {e}')
        return False

    try:
        #Delete S3 File
        s3.delete_object(Bucket=bucket, Key=result_filename)
    except Exception as e:
        print(f'Error deleting file form s3: {e}')
        return False
    
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table(config['dynamo']['Table'])
    sns = boto3.client('sns', region_name=config['aws']['AwsRegionName'])
    updated_response = table.get_item(Key={'job_id': job_data['job_id']})
    updated_data = updated_response['Item']
    sns_message = json.dumps({'default': json.dumps(updated_data)})
    
    try:
        sns_topic_response = sns.publish(
          TopicArn=config['sns']['GlacierArchive'],
          Message=sns_message,
          MessageStructure='json'
      )
        print('Message published to glacier archive sns')
    except Exception as e:
        print(f"Unable to publish sns message: {e}")
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
               
                if data['user_role'] == 'free_user':
                    current_time = int(time.time())
                    complete_time = int(float(data['complete_time']))

                    if (current_time - complete_time) > 300:
                        print('Archiving in Glacier')
                        success = glacierArchive(data)
                        if success:
                            receipt = msg['ReceiptHandle']
                            try:
                                sqs.delete_message(
                                    QueueUrl=sqs_url,
                                    ReceiptHandle=receipt
                                )
                            except Exception as e:
                                print(f'Error deleting message {e}')
                            result_file = data['s3_key_result_file']
                            print(f'Message {message_id} Deleted from Queue')
                            print(f'{result_file} moved to glacier archive')
                else:
                    #Delete message if premium user
                    try:
                        receipt = msg['ReceiptHandle']
                        sqs.delete_message(
                            QueueUrl=sqs_url,
                            ReceiptHandle=receipt
                        )
                        print(f'Message {message_id} Deleted from Queue')
                    except Exception as e:
                        print(f'Error deleting message {e}')

        time.sleep(10)

if __name__ == '__main__':
    sqs_url = config['sqs']['ArchiveUrl']
    SQS_message_reciever(sqs_url)

### EOF