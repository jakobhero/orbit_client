import warnings, datetime as dt, logging
from user import User
from google.cloud import bigquery

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
                user = User(email = row['email'], name = row['name'], github = row['github'], created_at = row['created_at'])
                logging.debug(f"retrieved User: {str(user)}.")
                self.result.append(user)

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