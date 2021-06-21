
import asyncio, aiohttp, logging
from data_clients import Integrator

class AsyncManager():
    def __init__(self, orbit):
        self.orbit = orbit

    async def process_batch(self,batch,rate_limit,timeout):
        """
        makes call to Orbit API to add users asynchronously and then streams the
        results into BigQuery.
        """

        async def stream_batch(batch):
            """
            parses user responses and streams them into BigQuery. As this is processed
            during a timeout in under to stay under the Orbit rate limit, sequential
            execution of the steps in this method do not cause a performance penality.
            """
            users, langs = [], []
            for row in batch:
                #skip rows where erroneous requests were made
                if row is None:
                    continue
                user_response, lang_response = self.orbit.parse_user_response(row)
                if user_response != None:
                    users.append(user_response)
                if lang_response != None:
                    for lang in lang_response:
                        langs.append(lang)

            #declaring jobs for the data injection
            jobs = [
                {
                    'name': 'User Injection',
                    'table': 'gitpod-growth.orbit.users',
                    'payload': users
                },
                {
                    'name': 'Languages Injection',
                    'table': 'gitpod-growth.orbit.languages',
                    'payload': langs
                }
            ]

            #executing the injection
            for job in jobs:
                logging.info(f"Executing Job '{job['name']}'...")
                integrator = Integrator(job['table'], job['payload'])
                integrator.execute()
                logging.info(f"Execution of Job '{job['name']}' has completed. {len(integrator.errors)} errors occurred.")
                for error in integrator.errors:
                    logging.error(error)

        async def set_timeout(mins):
            logging.info(f"Timing out for {mins} minute(s) in order to stay under the Orbit rate limit...")
            await asyncio.sleep(mins*60)
            logging.info(f"Timeout of {mins} minute(s) has completed. Resuming batch job...")        
        
        async with aiohttp.ClientSession() as session:
            lower = 0
            while lower < len(batch):
                sub_batch = batch[lower:(lower+rate_limit)]
                logging.info(f'Now starting to insert users from batch {lower+1}-{lower+len(sub_batch)} into Orbit...')
                tasks = [asyncio.ensure_future(self.orbit.async_add_member(session,user)) for user in sub_batch]

                response = await asyncio.gather(*tasks)
                logging.info(f'Insertion of batch {lower+1}-{lower+len(sub_batch)} into Orbit has completed.')
                
                #checking whether the current loop is the final (no timeout needed then)
                final_loop = lower + rate_limit >= len(batch)
                if not final_loop:
                    sleep = asyncio.create_task(set_timeout(timeout))
                processor = asyncio.create_task(stream_batch(response))

                #awaiting futures to complete
                await processor
                logging.info(f'Processing of batch {lower+1}-{lower+len(sub_batch)} has completed.')
                if not final_loop:
                    await sleep
                
                lower+=rate_limit

    def execute(self, batch, **kwargs):
        """
        executes asynchronous Orbit calls and BigQuery streaming inserts 
        by wrapping process_batch in asyncio event loop.
        """
        rate_limit = kwargs.get('limit', 120)
        timeout = kwargs.get('timeout', 1)
        asyncio.run(self.process_batch(batch,rate_limit,timeout))