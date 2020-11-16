from collections import Counter
from itertools import combinations
from os import stat
from twarc import Twarc
import requests
import sys
import os
import shutil
import io
import re
import json
from tqdm import tqdm
import glob
import multiprocessing
import pandas as pd
from datetime import datetime
from p_tqdm import p_map
from tweet_parser.tweet import Tweet
from tweet_parser.tweet_parser_errors import NotATweetError
import traceback
import urlexpander
import urllib as ulib
import mimetypes

# Variables for capturing stuff
tweets_captured = 0
influencer_frequency_dist = Counter()
mentioned_frequency_dist = Counter()
hashtag_frequency_dist = Counter()
url_frequency_dist = Counter()
user_user_graph = {}
user_hashtag_graph = {}
hashtag_hashtag_graph = {}
all_image_urls = []
tweets = {}
tweet_count = 0
save_dir = "./output/data"
JOIN_CHAR = '|'
TWEET_TYPE_ORIGINAL = 0
TWEET_TYPE_RETWEET = 1
TWEET_TYPE_REPLY = 2
TWEET_TYPE_QUOTE = 3
NUM_JOBS = 12

manager = multiprocessing.Manager()
bad_chars = manager.list()
# initializing bad_chars_list
bad_chars = ['<','>','?',':', ';', '\"', '!', "*",',','|', '\'']

#final_list = manager.list()
columns = ['id','created_at','text','source','tweet_type','source_tweet_id','retweeted_tweet_id',
           'quoted_tweet_id','quote_count','retweet_count','favorite_count','hashtags','most_unrolled_urls',
           'tweet_links','user_screen_name','user_id','user_mentions','user_followers_count',
           'user_following_count','user_listed_count','user_created_at','user_favourites_count','user_verified','user_statuses_count']
def get_user_mentions(status):
	interactions =[]
	try:
		for item in status.user_mentions:
			if item is not None:
				mention = item['id_str']
				if mention is not None and mention not in interactions:
					interactions.append(mention)
	except:
		pass
	
	return JOIN_CHAR.join(interactions)

# Returns a list of URLs found in the Tweet
def get_urls(links):
	urls = []
	try:
		for item in links:
			if item is not None:
				url = item['expanded_url']
				if url is not None and url not in urls:
					urls.append(url)
	
	except:
		pass
	return JOIN_CHAR.join(urls)

def get_hashtags(status):
	try:
		return JOIN_CHAR.join(status.hashtags)
	except:
		return None

def filter_images_and_expand(links):
    expanded_list = []
    for link in links:
        try:
            u = ulib.request.urlopen(link)
            link_type = u.headers['Content-Type'] #u.headers.gettype() # or using: u.info().gettype()
            if(link_type and 'image' not in link_type):
                expanded_list.append(u.geturl())
        except:
            #expanded_list.append(link)
            pass
        
    return ''.join(expanded_list)  
    
def clean_text(text):
    # stripr control chars
    stripped = ''.join(i for i in text if 31 < ord(i) < 127)
    stripped = ''.join((filter(lambda i: i not in bad_chars, stripped))).rstrip('\n').replace('\n',' ').replace(',','')
    return stripped
    
    
def flatten_status(tweet):
	tweet_dict = json.loads(tweet)
	status = Tweet(tweet_dict)
	flat_dict = {}
	flat_dict["id"] = status.id
	flat_dict['created_at'] = status.created_at_datetime
	flat_dict['text'] = clean_text(status.text)
	flat_dict['source'] = status['source']

	flat_dict['tweet_type'] = 'reply' if status.in_reply_to_status_id else status.tweet_type 
	flat_dict['source_tweet_id'] = status.in_reply_to_status_id if status.in_reply_to_status_id else (status.embedded_tweet.id if status.embedded_tweet else status.id)
	flat_dict['retweeted_tweet_id'] = status.retweeted_tweet.id if status.retweeted_tweet else None
	flat_dict['quoted_tweet_id'] = status.quoted_tweet.id if status.quoted_tweet else None
	flat_dict['quote_count'] = status.quote_count if status.quote_count else 0
	flat_dict['retweet_count'] = status.retweet_count if status.retweet_count else 0
	flat_dict['favorite_count'] = status.favorite_count if status.favorite_count else 0
	flat_dict['hashtags'] = get_hashtags(status)
	flat_dict['most_unrolled_urls'] = filter_images_and_expand(status.most_unrolled_urls)
	flat_dict['tweet_links'] = get_urls(status.tweet_links)
	

	### User Data
	if "user" in status:
		flat_dict['user_screen_name'] = status.screen_name
		flat_dict['user_id'] = status.user_id
		flat_dict['user_mentions'] = get_user_mentions(status)
		flat_dict['user_followers_count'] = status.follower_count
		flat_dict['user_following_count'] = status.following_count
		flat_dict['user_listed_count'] = status['user']['listed_count']
		flat_dict['user_created_at'] = status["user"]["created_at"]
		flat_dict['user_favourites_count'] = status.favorite_count if status.favorite_count else 0
		flat_dict['user_verified'] = status["user"]["verified"]
		flat_dict['user_statuses_count'] = status["user"]["statuses_count"] if status["user"]["statuses_count"] else 0
	return flat_dict

def process_file(targetfile):
    base_name = os.path.basename(targetfile)	
    outfile = os.path.join(save_dir, '{}.csv'.format(base_name))
    final_list = []
    with open(targetfile , 'r', encoding='utf-8') as f:		#lines = [next(f) for x in range(1000)]
        #lines = f.readlines()
        lines = [next(f) for x in range(100)]
        print('[[{}]] Start processing job......'.format(base_name))
        for idx, status in enumerate(lines):
            try:
                status = status.replace('\n',"").rstrip('\n')
                flattened_status = flatten_status(status)
                final_list.append(flattened_status)
            except NotATweetError:
                pass
            except:
                traceback.print_exc()
                print('{}-->{}'.format(idx, str(status)),end='\n')
                sys.exit()
    x = len(final_list)
    print('[[{}]] lines processed:{} of {}'.format(base_name, x, len(lines)))
    df_tweets = pd.DataFrame(final_list, columns=columns)
    df_tweets.to_csv(outfile, sep ='\t', index=False)
    print('[[{}]] Saved data to :{} '.format(base_name, outfile ))
    #shared_list.append(df_tweets)
    return (x , len(lines))


# Main starts here
if __name__ == '__main__':
	df_list = []
	processed_lines_all = 0
	total_lines = 0
	start_time = datetime.now()
	target = '/tmp/twitter-political/*17-json'
	files = glob.glob(target)
	#num_cpu = multiprocessing.cpu_count() - 2 if multiprocessing.cpu_count() > 2 else 1
	num_cpu = multiprocessing.cpu_count()
	print('num_cpu: {}'.format(num_cpu))
	#proccessed = p_map(process_file,files, num_cpus = num_cpu)
	with multiprocessing.Pool(processes=num_cpu) as p:    
		for res in tqdm(p.imap(process_file, files),total=len(files)):
			try:
				if res is not None:
					print('Number of lines proccessed:{}'.format(res[0]))
					processed_lines_all += res[0]
					total_lines += res[1]
			except:
				traceback.print_exc()
			
	print('Number of tweets proccessed:{} of {}'.format(processed_lines_all, total_lines))	
	total_time = datetime.now() - start_time
	print('total time for flattening data:{}'.format(total_time))
	# print('Concatanating dataframes...')
	# try:
	# 	start_time = datetime.now()
	# 	merged_df = pd.concat(shared_list, ignore_index=True)
	# 	merged_df.to_csv('./output/data/combined_data.csv.gzip',columns=columns, index=False, compression="gzip")
	# 	total_time = datetime.now() - start_time
	# 	print('total time for concatanating data:{}'.format(total_time))
	# except:
	# 	traceback.print_exc()
	
