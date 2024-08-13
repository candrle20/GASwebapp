import os
import subprocess
import boto3
import json

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ann_config.ini'))


class Annotator:

    def __init__(self):
        self.sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])
        self.url = config['sqs']['RequestUrl']


    def SQS_message_reciever(self):
        print(f'SQS Message Reciever Listening on {self.url}')
        while True:
            #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html
            sqs_response = self.sqs.receive_message(
                QueueUrl=self.url, 
                MaxNumberOfMessages=10,
                AttributeNames=['All'],
                WaitTimeSeconds=20
                )
            
            if 'Messages' in sqs_response:
                for msg in sqs_response['Messages']:
                    msg_body = json.loads(msg['Body'])
                    print(f"Received message: {msg_body}")
                    message_id = msg['MessageId']
                    success = self.process_message(msg_body)
                    if success:
                        receipt = msg['ReceiptHandle']
                        self.sqs.delete_message(
                            QueueUrl=self.url,
                            ReceiptHandle=receipt
                        )
                        print(f'Message {message_id} Deleted from Queue')


    def process_message(self, msg):
        msg_content = json.loads(msg['Message'])
        job_id = msg_content['job_id']
        bucket = msg_content['s3_inputs_bucket']
        object_key = msg_content['s3_key_input_file']
        
        try:
            self.S3_download(job_id, bucket, object_key)
        except Exception as e:
            print(f'Failed to download file from S3: {e}')
            return False
        try:
            self.run_anntools(job_id)
        except Exception as e:
            print(f'Failed to run anntools: {e}')
            return False

        return True


    def S3_download(self, job_id, bucket, object_key):
        #Download Annotation Files From S3
        s3 = boto3.client('s3', region_name=config['aws']['AwsRegionName'])
        os.makedirs('ann/anntools/data/jobs', exist_ok=True)
        download_path = os.path.join('ann/anntools/data/jobs', f'{job_id}.vcf')
        
        try:
            #https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-download-file.html
            s3.download_file(bucket, object_key, download_path)
            print('Download from S3 Successful')
        except Exception as e:
            print(f'Download from S3 Failed: {e}')


    def run_anntools(self, job_id):
        download_path = os.path.join('ann/anntools/data/jobs', f'{job_id}.vcf')
        #Run anntools on input file and update job status to running
        dynamo = boto3.resource('dynamodb')
        table = dynamo.Table(config['dynamo']['Table'])
        try:
            #https://stackoverflow.com/questions/63418641/dynamodb-boto3-conditional-update
            #HW4-4
            table.update_item(
                Key={'job_id': job_id},
                UpdateExpression='set job_status = :new',
                ExpressionAttributeValues={':new':'RUNNING', ':current':'PENDING'},
                ConditionExpression='job_status = :current',
                ReturnValues="UPDATED_NEW"
                )
            subprocess.Popen(['python3', 'ann/run.py', download_path])
        except Exception as e:
            table.update_item(
                Key={'job_id': job_id},
                UpdateExpression='set job_status = :correct',
                ExpressionAttributeValues={':false': 'RUNNING', ':correct':'PENDING'},
                ConditionExpression='job_status = :false',
                ReturnValues="UPDATED_NEW"
                )
            print(f'Failed to run anntools: {e}')

        print('Job Posted to Anntools')


if __name__ == '__main__':
    annot = Annotator()
    annot.SQS_message_reciever()