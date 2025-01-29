import os
import boto3
from datetime import datetime, timezone
import uuid
import time
from boto3.dynamodb.conditions import Key


# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
bedrock = boto3.client('bedrock')

print(f"boto3 version: {boto3.__version__}")


# Get environment variables
TABLE_NAME = os.environ['TABLE_NAME']
MODEL_ID = os.environ['MODEL_ID']
BATCH_JOB_S3_OUTPUT_URI = os.environ['BATCH_JOB_S3_OUTPUT_URI']
IAM_ROLE_ARN = os.environ['IAM_ROLE_ARN']
BATCH_JOB_NAME_PREFIX = os.environ.get('BATCH_JOB_NAME_PREFIX', 'embedding-job')
MAX_CONCURRENT_JOBS = int(os.environ.get('MAX_CONCURRENT_JOBS', '20'))  # Default to 3 if not set


PENDING_EMBEDDING_BATCH_STATUS = "Pending"

table = dynamodb.Table(TABLE_NAME)

def get_batch_inference_jobs(status_list: list[str]=["Submitted", "Validating", "Scheduled", "InProgress", "Stopping"], name_contains: str=BATCH_JOB_NAME_PREFIX):
    """
    Get batch inference jobs by status and name contains.
    """
    if not status_list:
        return []
    next_token = None
    invocations = []
    while True:
        if next_token is None:
            res = bedrock.list_model_invocation_jobs(
                nameContains=name_contains,
            )
        else:
            res = bedrock.list_model_invocation_jobs(
                nameContains=name_contains,
                nextToken=next_token
            )
            
        invocations.extend(res.get("invocationJobSummaries"))
        if "nextToken" in res: 
            next_token = res.get("nextToken")
        else:
            break

    return [inv for inv in invocations if inv.get("status") in status_list]

def get_pending_embedding_batch_records():
    """Query DynamoDB for pending jobs that haven't been started."""
    response = table.query(
        IndexName='status-index',
        KeyConditionExpression=Key('status').eq(PENDING_EMBEDDING_BATCH_STATUS)
    )
    return response.get('Items', [])

def create_batch_job(job_item):
    """Create a Bedrock batch inference job."""
    try:
        current_time = datetime.now(timezone.utc).isoformat()
        job_name = f"{BATCH_JOB_NAME_PREFIX}-{str(uuid.uuid4())[:12]}"
        
        response = bedrock.create_model_invocation_job(
            modelId=MODEL_ID,
            jobName=job_name,
            inputDataConfig={
                "s3InputDataConfig": {
                    "s3Uri": job_item['data_s3_uri']
                }
            },
            outputDataConfig={
                "s3OutputDataConfig": {
                    "s3Uri": BATCH_JOB_S3_OUTPUT_URI
                }
            },
            roleArn=IAM_ROLE_ARN
        )
        time.sleep(10)

        job_arn = response['jobArn']
        
        
        # Update DynamoDB with the job ARN and status
        table.update_item(
            Key={'id': job_item['id']},
            UpdateExpression='SET #status = :status, #job_arn = :arn, #created_dt = :created_dt',
            ExpressionAttributeNames={
                '#status': 'status',
                '#job_arn': 'batch_job_arn',
                '#created_dt': 'created_dt'
            },
            ExpressionAttributeValues={
                ':status': 'SUMMITTED',
                ':arn': job_arn,
                ':created_dt': current_time
            }
        )
        
        return True
    except Exception as e:
        print(f"Error creating batch job: {str(e)}")
        return False

def handler(event, context):
    """
    Lambda handler to monitor and manage batch inference jobs.
    """
    print("Starting batch job runner")
    print(f"Event: {event}")
    # Get current active jobs
    active_jobs = get_batch_inference_jobs()
    active_job_count = len(active_jobs)
    
    print(f"Current active jobs: {active_job_count}")
    
    # If we've reached max concurrent jobs, exit early
    if active_job_count >= MAX_CONCURRENT_JOBS:
        print(f"Maximum concurrent jobs ({MAX_CONCURRENT_JOBS}) reached. Exiting.")
        return {
            'statusCode': 200,
            'body': 'Maximum concurrent jobs reached'
        }
    
    # Calculate how many new jobs we can start
    jobs_to_start = MAX_CONCURRENT_JOBS - active_job_count
    print(f"Can start {jobs_to_start} new jobs")
    
    # Get pending jobs from DynamoDB
    pending_jobs = get_pending_embedding_batch_records()
    
    if not pending_jobs:
        print("No pending jobs found")
        return {
            'statusCode': 200,
            'body': 'No pending jobs to process'
        }
    
    # Start new batch jobs
    jobs_started = 0
    for job in pending_jobs[:jobs_to_start]:
        if create_batch_job(job):
            jobs_started += 1
    
    print(f"Started {jobs_started} new batch jobs. Active jobs: {active_job_count}")
    return {
        'statusCode': 200,
        'body': f'Started {jobs_started} new batch jobs. Active jobs: {active_job_count}'
    }
