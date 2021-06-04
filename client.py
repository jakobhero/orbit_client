from orbit import Orbit
from user import User
import os, json

orbit_token = os.environ.get('orbit_key')
workspace_id = "gitpod"

orbit = Orbit(orbit_token,workspace_id)
print(json.dumps(orbit.get_member('jakobhero')))