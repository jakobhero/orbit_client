import asyncio, requests, time
from timeit import default_timer
from concurrent.futures import ThreadPoolExecutor

async def get_data_asynchronous():
    numbers = [x for x in range(100)]

    with ThreadPoolExecutor(max_workers=100) as executor:
        with requests.Session() as session:
            # Set any session parameters here before calling `fetch`

            def func(number):
                time.sleep((100-number)/100)
                return number


            # Initialize the event loop        
            loop = asyncio.get_event_loop()
            
            # Set the START_TIME for the `fetch` function
            START_TIME = default_timer()
            
            # Use list comprehension to create a list of
            # tasks to complete. The executor will run the `fetch`
            # function for each csv in the csvs_to_fetch list
            tasks = [
                loop.run_in_executor(
                    executor,
                    func,
                    *(number) # Allows us to pass in multiple arguments to `fetch`
                )
                for number in numbers
            ]
            
            # Initializes the tasks to run and awaits their results
            for response in await asyncio.gather(*tasks):
                pass

def main():
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(get_data_asynchronous())
    loop.run_until_complete(future)

main()