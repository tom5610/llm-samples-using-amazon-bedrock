from aws_cdk import (
    # Duration,
    Stack,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as lambda_,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    # aws_sqs as sqs,
    RemovalPolicy,
    aws_s3 as s3,
    CfnOutput
)
from constructs import Construct

from aws_cdk import Duration

class SolutionStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create DynamoDB table
        table = dynamodb.Table(
            self, "EmbeddingBatchRegistry",
            table_name="embedding-batch-registry",
            partition_key=dynamodb.Attribute(
                name="id",
                type=dynamodb.AttributeType.STRING
            ),
            removal_policy=RemovalPolicy.DESTROY,  # For development - change for production
        )

        # Add Global Secondary Indexes
        table.add_global_secondary_index(
            index_name="status-index",
            partition_key=dynamodb.Attribute(
                name="status",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="created_dt",
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )

        table.add_global_secondary_index(
            index_name="batch-job-arn-index",
            partition_key=dynamodb.Attribute(
                name="batch_job_arn",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="created_dt",
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )

        # Create a S3 bucket for the batch inference job input & output
        bucket = s3.Bucket(
            self, "EmbeddingBatchJobBucket",
            bucket_name=f"embedding-batch-job-{Stack.of(self).account}-{Stack.of(self).region}",
            removal_policy=RemovalPolicy.DESTROY
        )

        # Construct the S3 output URI
        batch_job_s3_output_uri = f"s3://{bucket.bucket_name}/output/"

        # Create a IAM role for Bedrock to access S3
        bedrock_role = iam.Role(
            self, "BedrockBatchJobRole",
            assumed_by=iam.ServicePrincipal("bedrock.amazonaws.com"),
            description="IAM role for Bedrock batch jobs to access S3"
        )

        # Add S3 permissions to the role
        bedrock_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
            ],
            resources=[
                bucket.bucket_arn,
                f"{bucket.bucket_arn}/*"
            ]
        ))

        # Store the role ARN for use in Lambda environment
        iam_role_arn = bedrock_role.role_arn

        # Create Lambda layer with latest boto3
        boto3_layer = lambda_.LayerVersion(
            self, "Boto3Layer",
            layer_version_name="boto3-latest",
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            code=lambda_.Code.from_asset("lambda-layer"),
            description="Latest boto3 library"
        )

        # Create Lambda function
        batch_processor = lambda_.Function(
            self, "BatchJobRunner",
            function_name="batch-job-runner",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="runner.handler",
            code=lambda_.Code.from_asset("lambda"),
            layers=[boto3_layer],
            timeout=Duration.minutes(5),
            environment={
                "TABLE_NAME": table.table_name,
                "MODEL_ID": "amazon.titan-embed-text-v2:0",
                "BATCH_JOB_S3_OUTPUT_URI": batch_job_s3_output_uri,
                "IAM_ROLE_ARN": iam_role_arn,
                "MAX_CONCURRENT_JOBS": "20",
                "BATCH_JOB_NAME_PREFIX": "embedding-batch-job"
            }
        )

        # Grant Lambda permissions to access DynamoDB
        table.grant_read_write_data(batch_processor)

        # Grant Lambda permissions to access Bedrock
        batch_processor.add_to_role_policy(iam.PolicyStatement(
            actions=[
                "bedrock:CreateModelInvocationJob",
                "bedrock:GetModelInvocationJob",
                "bedrock:ListModelInvocationJobs",
            ],
            resources=["*"]
        ))

        batch_processor.add_to_role_policy(iam.PolicyStatement(
            actions=[
                "iam:PassRole",
            ],
            resources=[iam_role_arn]
        ))
        # Create EventBridge rule to trigger Lambda every 1 minute
        rule = events.Rule(
            self, "ScheduleRule",
            schedule=events.Schedule.rate(Duration.minutes(1))
        )

        # Add Lambda as target for the EventBridge rule
        rule.add_target(targets.LambdaFunction(batch_processor))

        # Create Status Monitor Lambda function
        status_monitor = lambda_.Function(
            self, "BatchInferenceStatusMonitor",
            function_name="batch-inference-status-monitor",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="monitor.handler",
            code=lambda_.Code.from_asset("lambda"),
            layers=[boto3_layer],
            environment={
                "TABLE_NAME": table.table_name
            }
        )

        # Grant Status Monitor Lambda permissions to access DynamoDB
        table.grant_read_write_data(status_monitor)

        # Create EventBridge rule for Bedrock Job status changes
        bedrock_status_rule = events.Rule(
            self, "BedrockStatusRule",
            event_pattern=events.EventPattern(
                source=["aws.bedrock"],
                detail_type=["Batch Inference Job State Change"],
                detail={
                    "status": ["Completed", "PartiallyCompleted", "Failed", "Stopped", "Expired"]
                }
            )
        )

        # Add Status Monitor Lambda as target for the Bedrock status rule
        bedrock_status_rule.add_target(targets.LambdaFunction(status_monitor))

        
        CfnOutput(self, "BatchInferenceRoleArn", value=bedrock_role.role_arn)
        CfnOutput(self, "DataS3BucketName", value=bucket.bucket_name)
        CfnOutput(self, "BatchRegistryTableName", value=table.table_name)