import os, logging, datetime as dt
from orbit import Orbit
from data_clients import Accessor
from async_manager import AsyncManager

def enrichment():
    """
    This method requests signup data from BigQuery and sends it to Orbit. In Orbit,
    the Signup data is enriched and the enriched data is then once again forwarded
    into BigQuery 
    """
    #query the data
    bq_job = Accessor(query=os.environ.get('bq_query'))
    bq_job.filter_time(lower_limit = dt.date(year=2021,month=6,day=28),upper_limit = dt.date(year=2021,month=6,day=30))
    bq_job.execute()
    logging.info(f"{len(bq_job.result)} users have been retrieved.")

    #create orbit instance
    orbit = Orbit(os.environ.get('orbit_key'),"gitpod")

    #create asynchronous batch processor and execute the batch job
    async_manager = AsyncManager(orbit)
    async_manager.execute(bq_job.result)

if __name__ == '__main__':
    logging.basicConfig(filename = 'debug.log', level = logging.DEBUG)
    enrichment()
