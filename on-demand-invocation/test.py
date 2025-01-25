import asyncio
from time import perf_counter as time
from pyrate_limiter import Duration, Limiter, Rate
from pyrate_limiter.buckets.in_memory_bucket import InMemoryBucket

limiter = Limiter([Rate(1999, Duration.MINUTE)], max_delay=3600)
n_requests = 27

async def limited_function(start_time):
    limiter.try_acquire('identity')
    print(f"t + {time() - start_time:.5f}")

async def test_ratelimit():
    start_time = time()
    tasks = [limited_function(start_time) for _ in range(n_requests)]
    await asyncio.gather(*tasks)
    print(f"Ran {n_requests} requests in {time() - start_time:.5f} seconds")


asyncio.run(test_ratelimit())