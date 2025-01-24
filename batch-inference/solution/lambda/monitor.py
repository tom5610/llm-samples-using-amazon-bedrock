import os
import boto3
from datetime import datetime
from datetime import timezone
from pprint import pprint

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])

def handler(event, context):
    # Extract relevant information from the event
    detail = event['detail']
    job_status = detail['status']
    job_arn = detail['batchJobArn']
    
    # Query DynamoDB using the job-arn-index to find the record
    response = table.query(
        IndexName='job-arn-index',
        KeyConditionExpression='job_arn = :job_arn',
        ExpressionAttributeValues={
            ':job_arn': job_arn
        }
    )
    pprint(f"DynamoDB query response: {response}")
    # Update the item with the new status
    if response['Items']:
        item = response['Items'][0]
        table.update_item(
            Key={'id': item['id']},
            UpdateExpression='SET #status = :status, #updated_at = :updated_at',
            ExpressionAttributeNames={
                '#status': 'status',
                '#updated_at': 'updated_at'
            },
            ExpressionAttributeValues={
                ':status': job_status,
                ':updated_at': datetime.now(timezone.utc).isoformat()
            }
        )
    else:
        print(f"[WARNING] No item found for job {job_arn}")
    return {
        'statusCode': 200,
        'body': f'Successfully updated status to {job_status} for job {job_arn}'
    }