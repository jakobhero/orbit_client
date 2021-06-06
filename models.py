import requests, json, re, warnings

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
            warnings.warn(f"Email '{str(email)}' is not valid. No email is set.")
    
    @property
    def name(self):
        return self.__name
    
    @name.setter
    def name(self, name):
        if isinstance(name, str):
            self.__name = name
        else:
            self.__name = None
            warnings.warn(f"Name requires a String datatype, passed was {type(name)}.")
    
    @property
    def github(self):
        return self.__github
    
    @github.setter
    def github(self, github):
        if isinstance(github, str):
            self.__github = github
        else: 
            self.__github = None
            warnings.warn(f"Github requires a String datatype, passed was {type(github)}.")

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
