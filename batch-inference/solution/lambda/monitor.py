import os
import boto3
from datetime import datetime
from datetime import timezone
from pprint import pprint
from boto3.dynamodb.conditions import Key


dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])

bedrock = boto3.client('bedrock')

def handler(event, context):
    # Extract relevant information from the event
    pprint(f"Event: {event}")
    pprint(f"context: {context}")
    detail = event['detail']
    job_status = detail['status']
    job_arn = detail['batchJobArn']
    
    # Query DynamoDB using the batch-job-arn-index to find the record
    response = table.query(
        IndexName='batch-job-arn-index',
        KeyConditionExpression=Key('batch_job_arn').eq(job_arn)
    )


    pprint(f"DynamoDB query response: {response}")
    # Update the item with the new status
    if response['Items']:
        # batch job record exists, hence, we may find the S3 output uri.
        response = bedrock.get_model_invocation_job(jobIdentifier=job_arn)
        output_data_s3_uri = response['outputDataConfig']['s3OutputDataConfig']['s3Uri'] + job_arn.split('/')[-1]

        # update the item with the new status, updated_at and output_data_s3_uri
        item = response['Items'][0]
        table.update_item(
            Key={'id': item['id']},
            UpdateExpression='SET #status = :status, #updated_at = :updated_at, #output_data_s3_uri = :output_data_s3_uri',
            ExpressionAttributeNames={
                '#status': 'status',
                '#updated_at': 'updated_at',
                '#output_data_s3_uri': 'output_data_s3_uri'
            },
            ExpressionAttributeValues={
                ':status': job_status,
                ':updated_at': datetime.now(timezone.utc).isoformat(),
                ':output_data_s3_uri': output_data_s3_uri
            }
        )
    else:
        print(f"[WARNING] No item found for job {job_arn}")
    return {
        'statusCode': 200,
        'body': f'Successfully updated status to {job_status} for job {job_arn}'
    }