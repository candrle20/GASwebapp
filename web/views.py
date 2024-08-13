# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
  request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Create a session client to the S3 service
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)
    
  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  # Get bucket name, key, and job ID from the S3 redirect URL
  bucket_name = str(request.args.get('bucket'))
  s3_key = str(request.args.get('key'))

  #Extract user id
  user_id = session.get('primary_identity')
  profile = get_profile(identity_id=user_id)

  # Extract the job ID from the S3 key
  file_name = s3_key.split('~')[1]
  split_job = s3_key.split('/')[2]
  job_id = split_job.split('~')[0]
  print(file_name, job_id)
  
  # Persist job to database
  data = { "job_id": job_id,
            "user_id": user_id,
            "username": profile.name,
            "user_role": profile.role,
            "email": profile.email,
            "input_file_name": file_name,
            "s3_inputs_bucket": bucket_name,
            "s3_key_input_file": s3_key,
            "submit_time": str(time.time()),
            "complete_time": 'N/A',
            "job_status": "PENDING"
            }
  print(f'Job Information: {data}')

  #Add Item to Table
  dynamo = boto3.resource('dynamodb')
  table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  try:
      table.put_item(Item=data)
      print(f'Job Information Added to Table {table}')
  except Exception as e:
    app.logger.error(f"Unable to add job information to table {table}: {e}")
    return abort(500)

  #Send message to request queue
  #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
  sns = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
  sns_message = json.dumps({'default': json.dumps(data)})
  try:
      sns_topic_response = sns.publish(
          TopicArn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'],
          Message=sns_message,
          MessageStructure='json'
      )
      return render_template('annotate_confirm.html', job_id=job_id)
  except Exception as e:
    app.logger.error(f"Unable to publish sns message: {e}")
    return abort(500)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():

  dynamo = boto3.resource('dynamodb')
  table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  user_id = session.get('primary_identity')

  try:
      #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/query.html
      data = table.query(
            IndexName='user_id_index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('user_id').eq(user_id),
            ProjectionExpression='job_id, input_file_name, submit_time, job_status'
          )
  except Exception as e:
    app.logger.error(f"Unable to get user annotations: {e}")
    return abort(500)

  #Make Times Human Readable
  for job in data['Items']:
    job['submit_time'] = datetime.fromtimestamp(float(job['submit_time'])).strftime('%Y-%m-%d %H:%M')
  
  return render_template('annotations.html', annotations=data['Items'])


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):

  dynamo = boto3.resource('dynamodb')
  table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  user_id = session.get('primary_identity')

  try:
    response = table.get_item(Key={'job_id': id})
    job_data = response.get('Item')
  except Exception as e:
    app.logger.error(f"Error fetching job data: {e}")
    return abort(500, description='Job not found')

  if not job_data:
    app.logger.error(f"Job ID {id} not found")
    return abord(404)

  if job_data['user_id'] != user_id:
    app.logger.error(f"Unauthorized access by user {user_id} for job {id}")
    return abort(403, description='Unauthorized user')
  
  s3 = boto3.client('s3', region_name=app.config['AWS_REGION_NAME'])
  bucket = app.config['AWS_S3_RESULTS_BUCKET']

  result_file_url = None
  submit_time = datetime.fromtimestamp(float(job_data['submit_time'])).strftime('%Y-%m-%d %H:%M')
  complete_time = None

  #Create Results Download Url
  try:
    if job_data['job_status'] == 'COMPLETED':
      #https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
      result_file_url = s3.generate_presigned_url('get_object',
                                                      Params={'Bucket': bucket,
                                                              'Key': job_data['s3_key_result_file']},
                                                      ExpiresIn=3600)
      complete_time = datetime.fromtimestamp(float(job_data['complete_time'])).strftime('%Y-%m-%d %H:%M')
  except Exception as e:
    app.logger.error(f"Error creating download url: {e}")
    return abort(500, description='Download url error')

  try:
    template_data = {
        "job_id": job_data['job_id'],
        "input_file_name": job_data['input_file_name'],
        "result_file_name": job_data.get('s3_key_result_file', 'N/A'),
        "result_file_url": result_file_url,
        "annotation_log": job_data.get('s3_key_log_file', 'N/A'),
        "complete_time": complete_time, 
        "submit_time": submit_time,
        "job_status": job_data['job_status']
      }
  except Exception as e:
    app.logger.error(f"Error showing job information: {e}")
    return abort(404)
  
  #Free Users Can Download For 5 Min
  free_access_expired = False
  if job_data['complete_time'] != 'N/A':
    if (int(time.time()) - int(float(job_data['complete_time']))) > 300:
      free_access_expired = True

  return render_template('annotation_details.html', annotation=template_data, free_access_expired=free_access_expired)

  

"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):

  dynamo = boto3.resource('dynamodb')
  table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  user_id = session.get('primary_identity')
  s3 = boto3.client('s3', region_name=app.config['AWS_REGION_NAME'])
  bucket_name = app.config['AWS_S3_RESULTS_BUCKET']
  
  response = table.get_item(Key={'job_id': id})
  job_data = response['Item']
  file_name = job_data["s3_key_log_file"]

  if not job_data:
    app.logger.error(f"Job ID {id} not found")
    return abort(404)

  if job_data['user_id'] != user_id:
    app.logger.error(f"Unauthorized User")
    return abort(403)
  

  try:
    log_file = s3.get_object(Bucket=bucket_name, Key=file_name)
    log_contents = log_file['Body'].read().decode('utf-8')
  except Exception as e:
    app.logger.error(f'Error accessing log file {e}')
    return abort(500)

  return render_template('view_log.html', log_file_contents=log_contents)



"""Subscription management handler
"""
@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"

    # Request restoration of the user's data from Glacier
    # Add code here to initiate restoration of archived user data
    # Make sure you handle files not yet archived!
    #Send message to request queue
    user_id = session.get('primary_identity')
    sns = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
    dynamo = boto3.resource('dynamodb')
    table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    
    #Get all User jobs from DynamoDB
    try:
      response = table.query(
            IndexName='user_id_index',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('user_id').eq(user_id)
          )
      app.logger.info(f'User {user_id} jobs retrieved')
    except Exception as e:
      app.logger.error(f"Unable to retrieve user {user_id}: {e}")
      return abort(500)

    #Restore Jobs in Glacier
    for job in response['Items']:
      if 'results_file_archive_id' in job.keys():
        sns_message = json.dumps({'default': json.dumps(job)})
        try:
          sns_topic_response = sns.publish(
              TopicArn=app.config['AWS_SNS_GLACIER_RESTORE_TOPIC'],
              Message=sns_message,
              MessageStructure='json'
          )
        except Exception as e:
          app.logger.error(f"Unable to initiate glacier restore: {e}")
          return abort(500)


    # Display confirmation page
    return render_template('subscribe_confirm.html') 

"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF
