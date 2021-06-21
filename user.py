import re, warnings, datetime as dt

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
        elif(re.search("^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*$", email)):
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
            out['created_at'] = str(self.created_at)
        return out
    
    def __str__(self):
        return f"User(email: {self.email}, name: {self.name}, github: {self.github}, created_at: {str(self.created_at)})"

