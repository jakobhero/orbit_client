import requests, json

class User:
    def __init__(self, **kwargs):
        self.email = kwargs.get('email', None)
        self.name = kwargs.get('name', None)
        self.github = kwargs.get('github', None)

    def get_id(self):
        """
        returns the identifier of the current user.
        """
        return self.github if self.github != None else self.email

class Orbit:
    def __init__(self, key, workspace):
        self.headers = {
            "Authorization": "Bearer " + key,
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        self.workspace = workspace
    
    def add_member(self, user):
        """
        adds the User user to the current Orbit Workspace
        """
        def user_parser(user):
            """
            returns a JSON representation of the user that can be passed to the orbit API.
            """
            data = {
                "member": {},
                "identity": {}
            }
            if user.github != None:
                data['identity']['source'] =  "github",
                data['identity']['username'] = user.github
                data['member']['email'] = user.email
            else:
                data['identity']['source'] = "email"
                data['identity']['email'] = user.email
            if user.name != None:
                data['member']['name'] = user.name
            return json.dumps(data)
                 
        data = user_parser(user)
        endpoint  = "https://app.orbit.love/api/v1/"+self.workspace+"/members"
        return requests.post(endpoint, data = data, headers = self.headers).json()

    def get_member(self, user_id):
        """
        retrieve the member with the provided user_id from the current Orbit Workspace.
        """
        endpoint =  "https://app.orbit.love/api/v1/"+self.workspace+"/members/"+user_id
        return requests.get(endpoint, headers = self.headers).json()
