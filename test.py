import unittest, os
from models import User, Orbit

class TestUser(unittest.TestCase):
    def test_valid_user(self):
        email = "jakob@gitpod.io"
        name = "jakob"
        github = "github"
        valid_user = User(email = email, name = name, github = github)
        self.assertEqual(email, valid_user.email)
        self.assertEqual(name, valid_user.name)
        self.assertEqual(github, valid_user.github)

    def test_invalid_email(self):
        email = "123.de"
        invalid_email_user = User(email = email)
        self.assertIsNone(invalid_email_user.email)
    
    def test_invalid_name(self):
        name = 123
        invalid_name_user = User(name = name)
        self.assertIsNone(invalid_name_user.name)

class TestOrbit(unittest.TestCase):
    def test_get(self):
        orbit_token = os.environ.get('orbit_key')
        workspace_id = "gitpod"
        orbit = Orbit(orbit_token,workspace_id)
        self.assertTrue(isinstance((orbit.get_member('jakobhero')), dict))

if __name__ == '__main__':
    unittest.main()