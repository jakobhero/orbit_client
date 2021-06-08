import os
from models import Accessor, Orbit, Integrator

def enrichment():
    """
    This method requests signup data from BigQuery and sends it to Orbit. In Orbit,
    the Signup data is enriched and the enriched data is then once again forwarded
    into BigQuery 
    """
    #query the data
    bq_job = Accessor(query=os.environ.get('bq_query'))
    bq_job.filter_time()
    bq_job.execute()

    #Send batch Job to Orbit
    orbit = Orbit(os.environ.get('orbit_key'),"gitpod")
    response = orbit.batch_job(orbit.add_member, bq_job.result)

    #parse the response
    users, langs = [], []
    for row in response:
        user_response, lang_response = orbit.parse_user_response(row)
        users.append(user_response)
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

    #executing injection
    for job in jobs:
        print(f"Executing Job '{job['name']}'...")
        integrator = Integrator(job['table'], job['payload'])
        integrator.execute()
        print(f"{len(integrator.errors)} errors occurred.")
        for error in integrator.errors:
            print(error)

if __name__ == '__main__':
    enrichment()
