import asyncio
import os
import sys
import argparse
import pandas as pd
import json
from time import perf_counter as time
from pyrate_limiter import Duration, Limiter, Rate
from pyrate_limiter.buckets.in_memory_bucket import InMemoryBucket
import boto3

rates = [Rate(1900, Duration.MINUTE)]
# basic_bucket = InMemoryBucket(rates)

limiter = Limiter(rates, max_delay=1000*60*60)

bedrock_runtime = boto3.client('bedrock-runtime')

MODEL_ID = 'amazon.titan-embed-text-v2:0'
DIMENSIONS = 256

async def limited_function(row, start_time):
    limiter.try_acquire(row['recordId'])

    sample_input = {
        "inputText": row['modelInput.inputText'],
        "dimensions": DIMENSIONS,
    }
    body = json.dumps(sample_input)
    response = bedrock_runtime.invoke_model(
        modelId=MODEL_ID,
        body=body,
        accept='application/json',
        contentType='application/json'
    )

    response_body = json.loads(response.get('body').read())
    embeddings = response_body['embedding']    
    print(f"t + {time() - start_time:.5f}")
    return row["recordId"], embeddings

async def invoke_model_with_ratelimit(df):
    start_time = time()
    tasks = [asyncio.create_task(limited_function(row, start_time)) for _, row in df.iterrows()]

    # wait for all tasks to complete
    await asyncio.wait(tasks)

    # Check status of each task
    completed = sum(1 for t in tasks if t.done() and not t.exception())
    errors = sum(1 for t in tasks if t.done() and t.exception())
    
    print(f"Completed: {completed}, Errors: {errors}")    
    print(f"Ran {len(df)} requests in {time() - start_time:.5f} seconds")

    results = [t.result()[0] for t in tasks if t.done() and not t.exception()]
    print(results)


if __name__ == "__main__":
    # validate the input argument must be an existing file
    # use argparse to get the input argument
    parser = argparse.ArgumentParser()
    parser.add_argument("data_file", type=str, help="The input file to process")
    args = parser.parse_args()
    if not os.path.exists(args.data_file):
        print(f"Error: {args.data_file} is not an existing file")
        sys.exit(1)
    
    # read the data file
    data = []
    with open(args.data_file) as f:
        for line in f:
            data.append(json.loads(line))
    df = pd.json_normalize(data, max_level=1)
    df = df.reset_index(drop=True)
    print(df.head())
    print(df.columns)
    
    asyncio.run(invoke_model_with_ratelimit(df))