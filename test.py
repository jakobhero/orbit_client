import unittest, os, warnings
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
    
    def test_batch(self):
        orbit_token = os.environ.get('orbit_key')
        workspace_id = "gitpod"
        orbit = Orbit(orbit_token,workspace_id)
        users = [
            User(name = "Test User 1", email = "test1@test.org", github = "jakob1"),
            User(name = "Test User 2", email = "test2@test.org", github = "jakob2")
        ]
        ids = [x.get_id() for x in users]
        
        #check whether IDs are already taken in orbit
        results = orbit.batch_job(orbit.get_member, ids)
        for result in results:
            if len(result.keys()) != 0:
                warnings.warn(f"Could not run unit test 'test_batch' as a user with id {result.get_id()} already exists.")
                return
        
        #check whether the correct number of users was added
        results = orbit.batch_job(orbit.add_member, users)
        count = 0
        for result in results:
            if len(result.keys()) != 0:
                count += 1
        self.assertEqual(count,len(users))

        # TODO: Delete does not work yet
        # #check whether correct number of users was deleted
        orbit.batch_job(orbit.delete_member, ids)
        results = orbit.batch_job(orbit.get_member, ids)
        count = 0
        for result in results:
            if len(result.keys()) != 0:
                count += 1
        self.assertEqual(count, 0)
                                
if __name__ == '__main__':
    unittest.main()