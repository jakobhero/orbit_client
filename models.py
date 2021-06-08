import requests, json, re, warnings, time
from google.cloud import bigquery
import datetime as dt

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
        if isinstance(name, str) or name == None:
            self.__name = name
        else:
            self.__name = None
            warnings.warn(f"'name' requires a String datatype, not {type(name)}. Name was set to None.")
    
    @property
    def github(self):
        return self.__github
    
    @github.setter
    def github(self, github):
        if isinstance(github, str) or github == None:
            self.__github = github
        else: 
            self.__github = None
            warnings.warn(f"'github' requires a String datatype, not {type(github)}. Github slug was set to None.")

    @property
    def created_at(self):
        return self.__created_at
    
    @created_at.setter
    def created_at(self, created_at):
        if isinstance(created_at, dt.datetime) or created_at == None:
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
        response = requests.post(endpoint, data = data, headers = self.headers)
        if response.ok:
            return response.json()
        else:
            return {}

    def get_member(self, user_id):
        """
        retrieve the member with the provided user_id from the current Orbit Workspace.
        """
        endpoint =  "https://app.orbit.love/api/v1/"+self.workspace+"/members/"+user_id
        response = requests.get(endpoint, headers = self.headers)
        if response.ok:
            return response.json()
        else:
            return {}

    def delete_member(self, user_id):
        """
        delete the member with the provided user_id from the current Orbit Workspace.
        """
        endpoint =  "https://app.orbit.love/api/v1/"+self.workspace+"/members/"+user_id
        response = requests.delete(endpoint, headers = self.headers)
        if response.ok:
            return response.json()
        else:
            return {}
    
    def batch_job(self, function, batch, **kwargs):
        rate_limit = kwargs.get('limit', 100)
        timeout = kwargs.get('timeout', 1)
        count = 0
        response = []
        for item in batch:
            response.append(function(item))
            count += 1
            if count % rate_limit == 0:
                print(f"{count} calls have been processed. Timing out for {timeout} seconds in order to stay under the rate limit")
                time.sleep(timeout)
        return response
    
    @staticmethod
    def parse_user_response(response, **kwargs):
        """
        extracts one dictionary for the user and languages used per user response. the attributes returned for the user can be
        specified by passing an array with the names in the keys keyword and renamed by passing a dictionary with the former
        name as keys and new names as values in the rename keyword. If no values are passed, defaults are assumed.
        """
        data = response['data']['attributes']
        default_keys = [
            "github",
            "name",
            "company",
            "location",
            "bio",
            "birthday",
            "love",
            "orbit_level",
            "activities_count",
            "reach",
            "github_followers",
            "twitter_followers",
            "twitter",
            "linkedin",
            "discourse",
            "email",
            "devto",
        ]
        keys = kwargs.get('keys',default_keys)
        default_rename = {
            "activities_count": "orbit_activities",
            "reach": "orbit_reach",
            "love": "orbit_love",
        }
        rename = kwargs.get('rename', default_rename)

        #only store data specified in keys in user_response
        user_response = {key: data[key] for key in keys}

        #rename dictionary keys
        for old_key in rename.keys():
            user_response[rename[old_key]] = user_response.pop(old_key)

        #parse languages
        lang_response, langs, github = [], data['languages'], user_response['github']
        if langs != None:
            for count, lang in enumerate(langs):
                lang_response.append({
                    "github": github,
                    "language": lang,
                    "rank": count + 1
                })
        
        return user_response,lang_response

class Accessor:
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

    def filter_time(self, **kwargs):
        """
        wraps the query of the BQJob in a time window. If 'column' is passed as keyword, the column passed as value will be
        used to filter time, else 'created_at' is taken as default. Pass a datetime in keyword 'lower_limit' and 'upper_limit'
        to select the lower (closed) bound and upper (open) bound of the time window. If no values are passed, 'lower_limit'
        defaults to midnight yesterday and 'upper_limit' defaults to midnight today. 
        """
        column = kwargs.get('column', 'created_at')
        today = dt.date.today()
        lower = kwargs.get('lower_limit', today - dt.timedelta(days = 1))
        upper = kwargs.get('upper_limit', today)

        lower, upper = lower.strftime('%Y-%m-%d'), upper.strftime('%Y-%m-%d')

        self.query = "SELECT * FROM (" + self.query + ") WHERE " + column + " >= \"" + lower + "\" AND " + column + " < \"" + upper + "\""

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

class Integrator:
    def __init__(self, table, payload):
        self.table = table
        self.payload = payload

    @property
    def table(self):
        return self.__table
    
    @table.setter
    def table(self, table):
        if isinstance(table, str):
            self.__table = table
        else:
            self.__table = None
            warnings.warn(f"Table must be passed as String, not {type(table)}. Table was set to None.")
    
    @property
    def payload(self):
        return self.__payload

    @payload.setter
    def payload(self, payload):
        if isinstance(payload, list):
            self.__payload = payload
        else:
            self.__payload = []
            warnings.warn(f"Payload must be passed as List, not {type(payload)}. Payload was set to [].")

    @property
    def errors(self):
        return self.__errors

    @errors.setter
    def errors(self, errors):
        if isinstance(errors, list):
            self.__errors = errors
        else:
            self.__errors = []
            warnings.warn(f"Errors must be passed as List, not {type(errors)}. Errors was set to [].")
    
    def execute(self):
        client = bigquery.Client()
        self.errors = client.insert_rows_json(self.table, self.payload, row_ids=[None] * len(self.payload))