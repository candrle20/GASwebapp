# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import sys
import time
import os
anntools_path = os.path.join(os.path.dirname(__file__), 'anntools')
sys.path.append(anntools_path)

import driver
import boto3
from flask import session
from configparser import ConfigParser
import json

current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, 'ann_config.ini')
config = ConfigParser(os.environ)
config.read(config_path)

"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
  def __init__(self, verbose=True):
    self.verbose = verbose

  def __enter__(self):
    self.start = time.time()
    return self

  def __exit__(self, *args):
    self.end = time.time()
    self.secs = self.end - self.start
    if self.verbose:
      print(f"Approximate runtime: {self.secs:.2f} seconds")

if __name__ == '__main__':
  # Call the AnnTools pipeline
  if len(sys.argv) > 1:
    with Timer():
      driver.run(sys.argv[1], 'vcf')

    #File and Job Information
    bucket = config['s3']['ResultsBucket']
    file_path = sys.argv[1]
    clean_path = file_path.split('.')[0]
    annot_file_path = f'{clean_path}.annot.vcf'
    log_file_path = f'{clean_path}.vcf.count.log'
    annot_file_name = os.path.basename(annot_file_path)
    log_file_name = os.path.basename(log_file_path)
    job_id = clean_path.split('/')[-1]

    s3 = boto3.client('s3', region_name=config['aws']['AwsRegionName'])
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table(config['dynamo']['Table'])
    response = table.get_item(Key={'job_id': job_id})
    user_id = response['Item']['user_id']
    folder_prexix = config['s3']['FolderPrefix']



    #Upload Files to S3
    s3_key_result = f'{folder_prexix}/{user_id}/{annot_file_name}'
    s3_key_log = f'{folder_prexix}/{user_id}/{log_file_name}'
    try:
      #https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
      s3.upload_file(annot_file_path, bucket, s3_key_result)
      s3.upload_file(log_file_path, bucket, s3_key_log)
      print('Files Uploaded Successfully')
    except Exception as e:
      print(f'S3 Upload Failed: {e}')
    
    #Update Dynamo DB Table
    try:
      #https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.UpdateItem.html
      table.update_item(
        Key={'job_id': job_id},
        UpdateExpression='''set s3_key_result_file = :results, 
                            s3_key_log_file = :log,
                            s3_results_bucket = :rbucket,
                            complete_time = :ct, 
                            job_status = :status''',
        ExpressionAttributeValues={
          ':results': s3_key_result,
          ':log': s3_key_log,
          ':rbucket': bucket,
          ':ct': str(time.time()),
          ':status': 'COMPLETED'
        },
        ReturnValues="UPDATED_NEW"
        )
      print(f'Job Information Added to Table {table}')
    except Exception as e:
      table.update_item(
            Key={'job_id': job_id},
            UpdateExpression='set job_status = :correct',
            ExpressionAttributeValues={':false': 'RUNNING', ':correct':'PENDING'},
            ConditionExpression='job_status = :false',
            ReturnValues="UPDATED_NEW"
            )
      print(f'DynamoDB Update Failed because {e}')
    
    #Send message to result topic
    updated_response = table.get_item(Key={'job_id': job_id})
    updated_data = updated_response['Item']
    #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
    sns = boto3.client('sns', region_name=config['aws']['AwsRegionName'])
    #https://stackoverflow.com/questions/34029251/aws-publish-sns-message-for-lambda-function-via-boto3-python2
    sns_message = json.dumps({'default': json.dumps(updated_data)})
    try:
      sns_topic_response = sns.publish(
          TopicArn=config['sns']['SnsResultsTopic'],
          Message=sns_message,
          MessageStructure='json'
      )
      print('Message published to sns results')
    except Exception as e:
      print(f"Unable to publish sns results message: {e}")
    
      
    #Remove Files From Annotator EC2 Instance
    try:
      os.remove(annot_file_path)
      os.remove(log_file_path)
      os.remove(file_path)
      print('Files Deleted Successfully')
    except Exception as e:
      print(f'Could Not Delete Files: {e}')

  else:
    print("A valid .vcf file must be provided as input to this program.")

### EOF