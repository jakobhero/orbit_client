import os
from models import BQJob

users = BQJob(query=os.environ.get('bq_query'))
users.execute()
for user in users.result:
    print(user.to_dict())