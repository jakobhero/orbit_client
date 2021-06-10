import os, logging, datetime as dt
from orbit import Orbit
from data_clients import Accessor, Integrator

def enrichment():
    """
    This method requests signup data from BigQuery and sends it to Orbit. In Orbit,
    the Signup data is enriched and the enriched data is then once again forwarded
    into BigQuery 
    """
    #query the data
    bq_job = Accessor(query=os.environ.get('bq_query'))
    bq_job.filter_time(lower_limit = dt.date(year=2021,month=6,day=1),upper_limit = dt.date.today()-dt.timedelta(days = 3))
    bq_job.execute()
    logging.info(f"{len(bq_job.result)} users have been retrieved.")

    #Send batch Job to Orbit
    orbit = Orbit(os.environ.get('orbit_key'),"gitpod")
    response = orbit.batch_job(orbit.add_member, bq_job.result)

    #parse the response
    users, langs = [], []
    for row in response:
        user_response, lang_response = orbit.parse_user_response(row)
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
        print(f"Executing Job '{job['name']}'...")
        integrator = Integrator(job['table'], job['payload'])
        integrator.execute()
        logging.info(f"{len(integrator.errors)} errors occurred.")
        for error in integrator.errors:
            logging.error(error)

if __name__ == '__main__':
    logging.basicConfig(filename = 'debug.log', level = logging.DEBUG)
    enrichment()
