
# coding: utf-8

# In[4]:


'''
FFF - Fetch Friends & Followers of all users
'''

import sys
import os
import math
import json
import time

import tweepy
from tweepy import Cursor


# In[7]:


def paginate(items,n):
    """
        Returns n-size chunks from items
    """
    for i in range(0,len(items),n):
        yield items[i:i+n]

def collect_friends_and_followers(client,fromUser,toUser):
    """
        Get followers count for a given user id
    """

    fp = open('/home/abagavat/SocialNetworksLab/Data/MultiplexNetworks/ElectionData/collect_friends_of_cache-1000000.txt', 'r')
#     op = open('Data/FamousUsers6.txt','a')

    for i, line in enumerate(fp):   
        if i < fromUser:
            continue

        if i == toUser:
            break

        print('Processing ' + str(i))
        dirname = '/home/abagavat/SocialNetworksLab/Data/Friends_Collection/{}'.format(line.split('\n')[0])

        try:
            os.makedirs(dirname, mode=0o755, exist_ok=True)
        except OSError:
            print('Directory {} already exists'.format(dirname))
        except Exception as e:
            print('Error while creating directory {}'.format(dirname))
            print(e)
            sys.exit(1)

        try:
#                 filename = 'Data/users1/{}/followers.json'.format(line.split('\n')[0])

                user = client.get_user(int(line.split('\n')[0]))
#                 MAX_FOLLOWERS = user.followers_count
                MAX_FRIENDS = user.friends_count
#                 max_pages = math.ceil(MAX_FOLLOWERS / 5000)

#             if MAX_FRIENDS < 100000 and MAX_FOLLOWERS < 100000:

#                 print("Getting " + str(MAX_FOLLOWERS) + " followers")
#                 with open(filename, 'w') as fp:
#                     for followers in Cursor(client.followers_ids, screen_name=user.screen_name).pages(max_pages):
#                         for chunk in paginate(followers, 100):
#                             while True:
#                                 try:
#                                     follower_chunk = client.lookup_users(user_ids=chunk)
#                                     break
#                                 except Exception as e:
#                                     print(e)
#                                     print('Sleeping 60 seconds')
#                                     time.sleep(60)

#                             for user in follower_chunk:
#                                 user_details = {}
#                                 user_details['user_id'] = user.id
#                                 user_details['screen_name'] = user.screen_name

#                                 fp.write(json.dumps(user_details))
#                                 fp.write("\n")

#                         if len(followers) == 5000:
#                             print('Entering sleep mode for 60 seconds to avoid rate limit')
#                             time.sleep(60)


                """
                    Get friends count for a given screen name
                """
                filename = '/home/abagavat/SocialNetworksLab/Data/Friends_Collection/{}/friends.json'.format(line.split('\n')[0])

                max_pages = math.ceil(MAX_FRIENDS / 5000)
                
                print("Getting " + str(MAX_FRIENDS) + " friends")
                with open(filename, 'w') as fp:
                    for friends in Cursor(client.friends_ids, screen_name=user.screen_name).pages(max_pages):
                        for chunk in paginate(friends, 100):
                            while True:
                                try:
                                    friend_chunk = client.lookup_users(user_ids=chunk)
                                    break
                                except Exception as e:
                                    print(e)
                                    print('Sleeping 60 seconds')
                                    time.sleep(60)

                            for user in friend_chunk:
                                user_details = {}
                                user_details['user_id'] = user.id
                                user_details['screen_name'] = user.screen_name

                                fp.write(json.dumps(user_details))
                                fp.write("\n")

                        if len(friends) == 5000:
                            print('Entering sleep mode for 60 seconds to avoid rate limit')
                            time.sleep(60)


#             else:
#                 op.write(line)
#                 continue

        except Exception as e:
            print(e)
            continue

    fp.close()


if __name__ == "__main__":
    fromUser = 190000
    toUser = 195000

    consumer_key = 'L0b6zAlcVlVeCXLD2CBwVTCMJ'
    consumer_secret = 'bxpp3Nid2cMKDxRJliON4k91ZuJBuzBEheGx2Yz5lSLGNHgwaf'
    access_token = '794732523779723264-9Vh7XbmZ8F1F5Lq3V1IF3GfYXI6Ex92'
    access_token_secret = 'ud4KjBGrH5hLz5p4tQtRsVD09dqp5gmrskrSIEhrwj9OI'

    auth = tweepy.OAuthHandler(consumer_key=consumer_key, consumer_secret=consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth_handler=auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    collect_friends_and_followers(api,fromUser,toUser)
