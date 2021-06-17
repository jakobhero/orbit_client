import asyncio, time

async def fetch_data(req_no):
    print(f'start fetching request number {str(req_no)}')
    await asyncio.sleep(2)
    print(f'done fetching request number {str(req_no)}')
    return {'data': req_no}

async def process_data(batch):
    print(f'Start to parse batch of length {len(batch)}.')
    await asyncio.sleep(5)
    print(f'Parsing of batch of length {len(batch)} has completed.')
    print(f'Starting to stream batch of length {len(batch)}.')
    await asyncio.sleep(3)
    print(f'Streaming of batch of length {len(batch)} has completed.')

async def batch_job(batch, **kwargs):
    rate_limit = kwargs.get('limit', 120)
    timeout = kwargs.get('timeout', 1)
    lower = 0
    while lower < len(batch):
        sub_batch = batch[lower:(lower+rate_limit)]
        tasks = [asyncio.create_task(fetch_data(x)) for x in sub_batch]
        results = []

        for i in range(len(tasks)):
            results.append(await tasks[i])

        print(f'Completed fetching users {lower+1}-{lower+len(results)}.')
        final_loop = lower + rate_limit >= len(batch)
        if not final_loop:
            set_timeout = asyncio.create_task(asyncio.sleep(timeout*60))
        processor = asyncio.create_task(process_data(sub_batch))

        await processor
        print(f'Waiting for {timeout} minute(s) in order to stay under rate limit...')
        if not final_loop:
            await set_timeout

        print(f'Processing and timeout for users {lower+1}-{lower+1+len(results)} completed.')

        lower+=rate_limit

start_time = time.time()
batch = [i for i in range(300)]
asyncio.run(batch_job(batch))
print(f'Programm finished in {round(time.time() - start_time, 2)} seconds.')