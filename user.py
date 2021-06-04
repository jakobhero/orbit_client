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