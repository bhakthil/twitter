
import requests
import os
import json
from tweet_parser.tweet import Tweet
from tweet_parser.tweet_parser_errors import NotATweetError
import fileinput
import tweepy as tw
import pandas as pd
import csv
import time

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'

#twitter_query = "conversation_id:1316019730407329798 AND (-is:retweet OR -is:quote)"
consumer_key = 'W97c2jdJv6ktaAmADKtckIpKH'
consumer_secret = 'LDgo6j3agJYCyP29mEaHycNI1ReyGk8AZVCToUCpwscRfFJpy6'
access_token = '87764411-CT5lsIlZc50NV3Wbtc7rQdosnpCBEa8NgNVE3Pm7n'
access_token_secret = 'zxtLT29Ns8XcB5TPpEq2ZIHIdqWbwnhrDoXlQhlSduua4'

search_words = ['black+lives+matter OR #BLM OR #BlackLivesMatter black lives matter ',
                'supreme court hearing OR Amy+Coney+Barrett OR Amy+Barrett',
                '#MeToo OR #ActToo OR Woman\'s+Rights OR Womans+Rights OR LGBTQ OR transgender+rights transgender',
                '2020+Election OR Vote OR #Election202 OR #EarlyVote OR #Vote OR Election',
                '#antivax OR #antivaxx OR #vaxxhappened OR vaccination OR anti-vaccination']

twitter_query = "{} min_faves:500 min_retweets:500 lang:en"
conversation_data = []
alltweets = []
ITEM_LIMIT=1

def handl_auth():
    auth = tw.OAuthHandler(consumer_key,consumer_secret)
    auth.set_access_token(access_token,access_token_secret)
    #auth = tw.AppAuthHandler(consumer_key, consumer_secret)
    #api = tw.API(auth, wait_on_rate_limit=True)
    api = tw.API(auth, wait_on_rate_limit=True)
    try:
        status = api.verify_credentials()
        return api
        #print(status)
    except Exception as e:
        print("Error creating API : {}".format(str(e)))
        raise e
        

def generate_ids():
    for search_word in search_words:
        new_search = twitter_query.format(search_word)
        api = handl_auth()
        new_tweets = tw.Cursor(api.search, q=new_search, lang="en").items(ITEM_LIMIT)
        #print(new_search, len([tweet for tweet in new_tweets]))
        alltweets.extend(new_tweets)

    outtweets = [[tweet.id, tweet.created_at, tweet.text.strip().rstrip('\n').replace('\n',''), tweet.retweet_count,tweet.favorite_count] for tweet in alltweets]
    # write the csv
    with open('./tweets_sample.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(["id","created_at","text","retweet_count","favorite_count"])
        writer.writerows(outtweets)


def main():
    generate_ids()

if __name__ == "__main__":
    main()
