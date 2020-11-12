
import requests
import os
import json
from searchtweets import collect_results, ResultStream, gen_request_parameters, load_credentials
from searchtweets.utils import write_result_stream
import fileinput
import csv
import pandas as pd
import time
from tweet_parser.tweet import Tweet
from tweet_parser.tweet_parser_errors import NotATweetError

def get_urls(links):
	urls = []
	for item in links:
		if item is not None:
			url = item['expanded_url']
			if url is not None and url not in urls:
				urls.append(url)
	return ','.join(urls)

def flatten_status(line):
	tweet_dict = json.loads(line)
	status = Tweet(tweet_dict)
	flat_dict = {}
	flat_dict["id"] = status.id
	flat_dict['created_at'] = status.created_at_datetime
	flat_dict['text'] = status.text.rstrip('\n').replace('\n',' ')
	flat_dict['source'] = status['source']

	#interaction_type, source_tweet  = get_interaction(status)
	flat_dict['tweet_type'] = 'reply' if status.in_reply_to_status_id else status.tweet_type 
	flat_dict['source_tweet_id'] = status.in_reply_to_status_id if status.in_reply_to_status_id else (status.embedded_tweet.id if status.embedded_tweet else None)
	flat_dict['retweeted_tweet_id'] = status.retweeted_tweet.id if status.retweeted_tweet else None
	flat_dict['quoted_tweet_id'] = status.quoted_tweet.id if status.quoted_tweet else None
	flat_dict['quote_count'] = status.quote_count
	flat_dict['retweet_count'] = status.retweet_count
	flat_dict['favorite_count'] = status.favorite_count
	flat_dict['hashtags'] = ','.join(status.hashtags)
	flat_dict['most_unrolled_urls'] = ','.join(status.most_unrolled_urls)
	flat_dict['tweet_links'] = get_urls(status.tweet_links)


	### User Data
	if "user" in status:
		flat_dict['user_screen_name'] = status.screen_name
		flat_dict['user_id'] = status.user_id
		flat_dict['user_mentions'] = status.user_mentions
		flat_dict['user_followers_count'] = status.follower_count
		flat_dict['user_following_count'] = status.following_count
		flat_dict['user_listed_count'] = status['user']['listed_count']
		flat_dict['user_created_at'] = status["user"]["created_at"]
		flat_dict['user_favourites_count'] = status.favorite_count
		flat_dict['user_verified'] = status["user"]["verified"]
		flat_dict['user_statuses_count'] = status["user"]["statuses_count"]
	return flat_dict

def do_process():
    with open('/tmp/twitter-political/large-17-json', 'r', encoding='utf-8') as f:
        head = [next(f) for x in range(1000)]
        
        #head = f.readlines()
        for strline in head:
            try:
                _dict = flatten_status(strline)
                keylist = list(_dict.keys())
                print('\',\''.join(keylist))
            except (json.JSONDecodeError,NotATweetError):
                pass
bad_chars = [';', '\"', '!', "*",',','|', '\'']
    
def clean_text(text):
    # stripr control chars
    stripped = ''.join(i for i in text if 31 < ord(i) < 127)
    stripped = ''.join((filter(lambda i: i not in bad_chars, stripped))).rstrip('\n').replace('\n',' ').replace(',','')
    return stripped   
        
def main():
    #do_process()
    x =r'Obama nominates John Kerry as secretary of state http//t.co/k4qlZY2n ;:!*,|	"<a href=""http://twitter.com/tweetbutton"" rel=""nofollow"">Tweet Button</a>"'
    x = clean_text(x)
    print(x)
        
                
         
        
if __name__ == "__main__":
    main()
