
# Welcome to CDK project for Processing Large Dataset using Amazon Bedrock Batch Inference

> **Note**: The solution is backed by [AWS CDK](https://docs.aws.amazon.com/cdk/v2/guide/home.html), an open-source software development framework for defining cloud infrastructure in code and provisioning it through AWS CloudFormation. In addition, Python is being used to create the stack.

## Steps

1. Create a lambda layer using latest boto3 version.
  > **Note**: The built-in boto3 version in AWS Lambda Python 3.12 runtime doesn't provide the latest API supports for Bedrock Batch Inference, hence, we bake our layer with the latest boto3 version and will be using it in our Lambda functions.

```
mkdir -p lambda-layer/python
cd lambda-layer/python
pip install boto3 -t .
cd ../..
```

2. Configure your AWS environment

Please create a AWS profile at your environment with proper permissions to create the resources. Your may explicitly setup env variable `AWS_DEFAULT_REGION` before deploying the stack. For more details, you may refer to [Configure security credentials for the AWS CDK CLI](https://docs.aws.amazon.com/cdk/v2/guide/configure-access.html) and [Configure environments to use with the AWS CDK](https://docs.aws.amazon.com/cdk/v2/guide/configure-env.html)

3. Boostrap your AWS environment

Please run the following command to bootstrap your AWS environment. Please replace the `123456789012` with your AWS account id, and `us-east-1` with your preferred region.

```
cdk bootstrap aws://123456789012/us-east-1
```

For more details, please refer to [Bootstrap your environment for use with the AWS CDK](https://docs.aws.amazon.com/cdk/v2/guide/bootstrapping-env.html)

4. Deploy the stack

Run the following command to deploy the stack.

```
cdk deploy
```

For more details, please refer to [Deploy AWS CDK applications](https://docs.aws.amazon.com/cdk/v2/guide/deploy.html)

## Next Step

Once the stack is deployed, you may run the batch registry notebook to prepare the data. 