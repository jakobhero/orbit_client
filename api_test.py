import requests, json, os

#requires valid api token in orbit_key variable
api_token = os.environ.get('orbit_key')
workspace_id = "gitpod"

data = {
  "member": {
    "name": "Jakob Test"
  },
  "identity": {
    "source": "github",
    "username": "jakobhero-test2"
  }
}

endpoint = "https://app.orbit.love/api/v1/"+workspace_id+"/members"
headers = {
    "Authorization": "Bearer " + api_token,
    "Accept": "application/json",
    "Content-Type": "application/json"
    }
print(requests.post(endpoint, data=json.dumps(data), headers=headers).json())