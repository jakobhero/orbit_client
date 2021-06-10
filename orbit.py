import logging, requests, time, json, aiohttp, asyncio

class Orbit:
    def __init__(self, key, workspace):
        self.headers = {
            "Authorization": "Bearer " + key,
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        self.workspace = workspace
    
    def user_parse(self, user):
        """
        returns a JSON representation of the user that can be passed to the orbit API.
        """
        member ={}
        if user.github != None:
            member['github'] = user.github
        if user.email != None:
            member['email'] = user.email
        if user.name != None:
            member['name'] = user.name
        return json.dumps({"member": member})

    def add_member(self, user):
        """
        adds a user to the current Orbit Workspace
        """
                 
        data = self.user_parse(user)
        logging.debug(f"Parsed User {str(user)} to the following request body:\n{data}")
        endpoint  = "https://app.orbit.love/api/v1/"+self.workspace+"/members"
        response = requests.post(endpoint, data = data, headers = self.headers)
        if response.ok:
            out = response.json()
            logging.debug(f"Sent User {str(user)} to orbit and received the following response:\n{str(out)}")
            return out
        else:
            logging.debug(f"Sent User {str(user)} to orbit and Error occurred:\n{str(response)}")
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
        """
        executes a any orbit function for each element passed in batch. in keywords a
        limit for consecutive calls before a setting a timeout can be specified.
        """
        rate_limit = kwargs.get('limit', 100)
        timeout = kwargs.get('timeout', 0)
        count = 0
        response = []
        for item in batch:
            response.append(function(item))
            count += 1
            if count % rate_limit == 0:
                print(f"{count} calls have been processed. Timing out for {timeout} minute(s) in order to stay under the rate limit")
                time.sleep(timeout)
        return response

    #TODO figure out async process downstream: what to pass as callback, how to manage request limit
    # def add_batch(self,users, **kwargs):
    #     """
    #     adds all members passed in users to orbit with asynchronous calls. 'limit' and
    #     'timeout' passed in the keywords manage how many asynchronous requests at most
    #     are made before a specified timeout is called.
    #     """
    #     endpoint  = "https://app.orbit.love/api/v1/"+self.workspace+"/members"

    #     async def post(data, session):
    #         try:
    #             async with session.post(endpoint, headers = self.headers, data = data) as response:
    #                 return response
    #         except Exception as e:
    #             logging.debug(f"Error occurred!\n{str(e)}")

    #     async def manage(users, limit, timeout):
    #         results = []
    #         count,errors = 0,0
    #         while count < len(users):
    #             batch = users[count:count+limit]
    #             logging.debug(f'Sending requests {count+1}-{count+1+limit}...')
    #             async with aiohttp.ClientSession() as session:
    #                 response = await asyncio.gather(*[post(self.user_parse(user), session) for user in batch])
    #                 if response.ok:
    #                     logging.debug(f"Received OK Response:\n{str(response)}")
    #                     results.add(response.json())
    #                 else:
    #                     errors += 1
    #                     logging.debug(f"Error occurred!\n{str(response)}")
    #             count+=limit
    #             if count < len(users):
    #                 logging.debug(f'Taking a timeout of {timeout} minute(s)...')
    #                 time.sleep(timeout*60)
    #         return results, errors
        
    #     limit = kwargs.get('limit', 100)
    #     timeout = kwargs.get('timeout', 1)*60
    #     results, errors = manage(users,limit,timeout)
    #     logging.info(f'Finished the orbit insert job. {len(results)} users were inserted or updated successfully. {errors} users failed.')
    #     return results

    @staticmethod
    def parse_user_response(response, **kwargs):
        """
        extracts one dictionary for the user and languages used per user response. the attributes returned for the user can be
        specified by passing an array with the names in the keys keyword and renamed by passing a dictionary with the former
        name as keys and new names as values in the rename keyword. If no values are passed, defaults are assumed.
        """
        if 'data' not in response:
            return None, None
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