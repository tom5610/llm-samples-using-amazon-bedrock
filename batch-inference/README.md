# Batch Processing Large-scale Data using Amazon Bedrock

## The problem

Imagine we are processing certain million of records in a dataset, and we are trying to get inference result from a leading LLM model. Furthermore, we want to get the result as soon as possible. We may evaluate realtime inference vs batch inference, with considering the service quotas (limits) on the services. 

e.g. 100M records to get embedding from Amazon Titan Text Embedding V2 using Amazon Bedrock. 

## The solution

Divide and Conquer is the key. We may split the dataset into smaller chunks, and process them in parallel. The potential best option can be using Amazon Bedrock Batch Inference job, which may tackle the scalability and resilience of error handling.  

