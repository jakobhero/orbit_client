import os, json

#store credentials in local file
#requires that credentials are stored in environment variable 'bq_credentials'
with open('bq_credentials.json', 'w') as outfile:
    json.dump(json.loads(os.environ.get('bq_credentials')), outfile)