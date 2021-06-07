import requests, json, re, warnings
from google.cloud import bigquery
from datetime import datetime as dt

class User:
    def __init__(self, **kwargs):
        self.email = kwargs.get('email', None)
        self.name = kwargs.get('name', None)
        self.github = kwargs.get('github', None)
        self.created_at = kwargs.get('created_at', None)

    def get_id(self):
        """
        returns the identifier of the current user.
        """
        return self.github if self.github != None else self.email

    @property
    def email(self):
        return self.__email
    
    @email.setter
    def email(self, email):
        #don't check for validity if user does not have email
        if email == None:
            self.__email = None
        #check for email validity with regex
        elif(re.search('^(\w|\.|\_|\-)+[@](\w|\_|\-|\.)+[.]\w{2,3}$', email)):
            self.__email = email
        else:
            self.__email = None
            warnings.warn(f"Email '{str(email)}' is not valid. Email was set to None.")
    
    @property
    def name(self):
        return self.__name
    
    @name.setter
    def name(self, name):
        if isinstance(name, str):
            self.__name = name
        else:
            self.__name = None
            warnings.warn(f"'name' requires a String datatype, not {type(name)}. Name was set to None.")
    
    @property
    def github(self):
        return self.__github
    
    @github.setter
    def github(self, github):
        if isinstance(github, str):
            self.__github = github
        else: 
            self.__github = None
            warnings.warn(f"'github' requires a String datatype, not {type(github)}. Github slug was set to None.")

    @property
    def created_at(self):
        return self.__created_at
    
    @created_at.setter
    def created_at(self, created_at):
        if isinstance(created_at, dt):
            self.__created_at = created_at
        else:
            self.__created_at = None
            warnings.warn(f"'created_at' requires a DateTime datatype, not {type(created_at)}. The attribute was set to None")
    
    def to_dict(self):
        out = {}
        if self.email != None:
            out['email'] = self.email
        if self.name != None:
            out['name'] = self.name
        if self.github != None:
            out['github'] = self.github
        if self.created_at != None:
            out['created_at'] = self.created_at
        return out

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
        adds a user to the current Orbit Workspace
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

class BQJob:
    def __init__(self, **kwargs):
        self.query = kwargs.get('query', None)
        self.result = None
    
    @property
    def query(self):
        return self.__query
    
    @query.setter
    def query(self, query):
        if isinstance(query, str):
            self.__query = query
        else:
            self.__query = None
            warnings.warn(f"Query must be passed as String, not {type(query)}. Query was set to None.")
        
    @property
    def result(self):
        return self.__result
    
    @result.setter
    def result(self, result):
        self.__result = result

    def execute(self):
        """
        This method executes queries against a user table that contains the columns 'name', 'email', 'github', and 'created_at'.
        """
        if self.query == None:
            warnings.warn("BQJob contains no query to execute. Please attach a query.")
        else:
            client = bigquery.Client()
            self.result = []
            for row in client.query(self.query):
                self.result.append(User(email = row['email'], name = row['name'], github = row['github'], created_at = row['created_at']))
    

            

