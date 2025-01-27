import asyncio
from time import perf_counter as time
import os
import sys
import json
import argparse
import pandas as pd
from typing import List, Any
from dataclasses import dataclass
import boto3
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from functools import partial
import time as time_module  # Renamed to avoid conflict with perf_counter

bedrock_runtime = boto3.client('bedrock-runtime')

start_time = time()

# Add rate limiting constants
RATE_LIMIT_CALLS = 2000  # calls
RATE_LIMIT_PERIOD = 60  # seconds
# CALL_DELAY = RATE_LIMIT_PERIOD / (RATE_LIMIT_CALLS) * 3   # seconds per call

CALL_DELAY = RATE_LIMIT_PERIOD / RATE_LIMIT_CALLS * 0.1
# CALL_DELAY -= 0.100

MODEL_ID = 'amazon.titan-embed-text-v2:0'
DIMENSIONS = 256

def process_item_sync(item: Any) -> dict:
    """Synchronous version of process_item for multiprocessing."""
    input_text = item['modelInput.inputText']
    sample_input = {
        "inputText": input_text,
        "dimensions": DIMENSIONS,
    }
    body = json.dumps(sample_input)
    
    # Add rate limiting delay
    # time_module.sleep(CALL_DELAY)
    
    response = bedrock_runtime.invoke_model(
        modelId=MODEL_ID,
        body=body,
        accept='application/json',
        contentType='application/json'
    )

    response_body = json.loads(response.get('body').read())
    embeddings = response_body['embedding']    
    print(f"t + {time() - start_time:.5f}")
    return item["recordId"], input_text, embeddings

def process_batch(dataframe: pd.DataFrame, batch_size: int = 2000) -> List[dict]:
    """Process items concurrently with rate limiting and multiple processors."""
    
    # Calculate optimal number of processes (leave one core free)
    num_processes = max(1, multiprocessing.cpu_count() - 1)
    
    results = []
    for i in range(0, len(dataframe), batch_size):
        batch_df = dataframe.iloc[i:i + batch_size]
        print(f"Processing batch {i // batch_size + 1} of {len(dataframe) // batch_size}")
        
        # Process the batch using multiple processes
        with ProcessPoolExecutor(max_workers=num_processes) as executor:
            batch_results = list(executor.map(
                process_item_sync,
                batch_df.to_dict(orient="records")
            ))
        results.extend(batch_results)
    
    return results

async def main():
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


    print("Starting processing...")
    start_time = time()
    
    results = process_batch(df)
    
    end_time = time()
    print(f"Processed {len(results)} items in {end_time - start_time:.2f} seconds")
    print(f"Average rate: {len(results) / (end_time - start_time):.2f} items/second")

    
    with open("./output.jsonl", 'w') as f:
        for record_id, input_text, embeddings in results:
            result_dict = {
                'recordId': record_id,
                'inputText': input_text,
                'embeddings': embeddings
            }
            f.write(json.dumps(result_dict) + '\n')
            

if __name__ == "__main__":
    asyncio.run(main())
