from collections import Counter
from itertools import combinations
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
import traceback
import urllib as ulib

# # Variables for capturing stuff
# tweets_captured = 0
# influencer_frequency_dist = Counter()
# mentioned_frequency_dist = Counter()
# hashtag_frequency_dist = Counter()
# url_frequency_dist = Counter()
# user_user_graph = {}
# user_hashtag_graph = {}
# hashtag_hashtag_graph = {}
# all_image_urls = []
# tweets = {}
#tweet_count = 0
save_dir = "./output/"
  
# Helper functions for saving json, csv and formatted txt files
def save_json(variable, filename):
	with io.open(filename, "w", encoding="utf-8") as f:
		f.write(json.dumps(variable, indent=4, ensure_ascii=False))
		
def save_csv(data, filename):
	with io.open(filename, "w", encoding="utf-8") as handle:
		handle.write(u"Source,Target,Weight\n")
		for source, targets in sorted(data.items()):
			for target, count in sorted(targets.items()):
				if source != target and source is not None and target is not None:
					handle.write(source + u"," + target + u"," + str(count) + u"\n")

def save_text(data, filename):
	with io.open(filename, "w", encoding="utf-8") as handle:
		for item, count in data.most_common() :
			if item is not None:
				try:
					handle.write(str(count) + "\t" + item + "\n")
				except Exception as e:
					print(item, count)
					raise e
       
# Returns the screen_name of the user retweeted, or None
def retweeted_user(status):
	if "retweeted_status" in status:
		orig_tweet = status["retweeted_status"]
		if "user" in orig_tweet and orig_tweet["user"] is not None:
			user = orig_tweet["user"]
			if "screen_name" in user and user["screen_name"] is not None:
				return user["screen_name"]

# Returns a list of screen_names that the user interacted with in this Tweet
def get_interactions(status):
	interactions = []
	if "in_reply_to_screen_name" in status:
		replied_to = status["in_reply_to_screen_name"]
		if replied_to is not None and replied_to not in interactions:
			interactions.append(replied_to)
	if "retweeted_status" in status:
		orig_tweet = status["retweeted_status"]
		if "user" in orig_tweet and orig_tweet["user"] is not None:
			user = orig_tweet["user"]
			if "screen_name" in user and user["screen_name"] is not None:
				if user["screen_name"] not in interactions:
					interactions.append(user["screen_name"])
	if "quoted_status" in status:
		orig_tweet = status["quoted_status"]
		if "user" in orig_tweet and orig_tweet["user"] is not None:
			user = orig_tweet["user"]
			if "screen_name" in user and user["screen_name"] is not None:
				if user["screen_name"] not in interactions:
					interactions.append(user["screen_name"])
	if "entities" in status:
		entities = status["entities"]
		if "user_mentions" in entities:
			for item in entities["user_mentions"]:
				if item is not None and "screen_name" in item:
					mention = item['screen_name']
					if mention is not None and mention not in interactions:
						interactions.append(mention)
	return interactions

# Returns a list of hashtags found in the tweet
def get_hashtags(status):
	hashtags = []
	if "entities" in status:
		entities = status["entities"]
		if "hashtags" in entities:
			for item in entities["hashtags"]:
				if item is not None and "text" in item:
					hashtag = item['text']
					if hashtag is not None and hashtag not in hashtags:
						hashtags.append(hashtag)
	return hashtags

# Returns a list of URLs found in the Tweet
def get_urls(status):
	urls = []
	if "entities" in status:
		entities = status["entities"]
		if "urls" in entities:
			for item in entities["urls"]:
				if item is not None and "expanded_url" in item:
					url = item['expanded_url']
					if url is not None and url not in urls:
						urls.append(url)
	return urls

# Returns a list of expanded non-image URLs found in the Tweet
def filter_images_and_expand(links):
    expanded_list = []
    for link in links:
        try:
            u = ulib.request.urlopen(link)
            link_type = u.headers['Content-Type'] #u.headers.gettype() # or using: u.info().gettype()
            if(link_type and 'image' not in link_type):
                expanded_list.append(u.geturl())
        except:
            pass
        
    return expanded_list


# Returns the URLs to any images found in the Tweet
def get_image_urls(status):
	urls = []
	if "entities" in status:
		entities = status["entities"]
		if "media" in entities:
			for item in entities["media"]:
				if item is not None:
					if "media_url" in item:
						murl = item["media_url"]
						if murl not in urls:
							urls.append(murl)
	return urls

def process_status(status, influencer_frequency_dist, 
					mentioned_frequency_dist, hashtag_frequency_dist, 
					url_frequency_dist,user_user_graph, user_hashtag_graph,
					hashtag_hashtag_graph, tweets):
	screen_name = None
	if "user" in status:
		if "screen_name" in status["user"]:
			screen_name = status["user"]["screen_name"]

	retweeted = retweeted_user(status)
	if retweeted is not None:
		influencer_frequency_dist[retweeted] += 1
	else:
		influencer_frequency_dist[screen_name] += 1

# Tweet text can be in either "text" or "full_text" field...
	text = None
	if 'extended_tweet' in status:
		text = status['extended_tweet']['full_text']
	elif "full_text" in status:
		text = status["full_text"]
	elif "text" in status:
		text = status["text"]
	if text:
		text = text.rstrip('\n').replace('\n',' ')
	id_str = None
	if "id_str" in status:
		id_str = status["id_str"]

# Assemble the URL to the tweet we received...
	tweet_url = None
	try:
		if id_str is not None and screen_name is not None:
			tweet_url = "https://twitter.com/" + screen_name + "/status/" + str(id_str)
	except Exception as e:
		traceback.print_exc()
		print(id_str,tweet_url)
		raise e
# ...and capture it
	if tweet_url is not None and text is not None:
		tweets[tweet_url] = text

# Record mapping graph between users
	interactions = get_interactions(status)
	if interactions is not None:
		for user in interactions:
			mentioned_frequency_dist[user] += 1
			if screen_name not in user_user_graph:
				user_user_graph[screen_name] = {}
			if user not in user_user_graph[screen_name]:
				user_user_graph[screen_name][user] = 1
			else:
				user_user_graph[screen_name][user] += 1 

# Record mapping graph between users and hashtags
	hashtags = get_hashtags(status)
	if hashtags is not None:
		if len(hashtags) > 1:
			hashtag_interactions = []
# This code creates pairs of hashtags in situations where multiple
# hashtags were found in a tweet
# This is used to create a graph of hashtag-hashtag interactions
			for comb in combinations(sorted(hashtags), 2):
				hashtag_interactions.append(comb)
			if len(hashtag_interactions) > 0:
				for inter in hashtag_interactions:
					item1, item2 = inter
					if item1 not in hashtag_hashtag_graph:
						hashtag_hashtag_graph[item1] = {}
					if item2 not in hashtag_hashtag_graph[item1]:
						hashtag_hashtag_graph[item1][item2] = 1
					else:
						hashtag_hashtag_graph[item1][item2] += 1
			for hashtag in hashtags:
				hashtag_frequency_dist[hashtag] += 1
				if screen_name not in user_hashtag_graph:
					user_hashtag_graph[screen_name] = {}
				if hashtag not in user_hashtag_graph[screen_name]:
					user_hashtag_graph[screen_name][hashtag] = 1
				else:
					user_hashtag_graph[screen_name][hashtag] += 1

	urls = get_urls(status)
	#filter out images and expand the urls
	#urls = filter_images_and_expand(urls)
	if urls is not None:
		for url in urls:
			url_frequency_dist[url] += 1



# Main starts here

def process_file(targetfile):
	# Variables for capturing stuff
	influencer_frequency_dist = Counter()
	mentioned_frequency_dist = Counter()
	hashtag_frequency_dist = Counter()
	url_frequency_dist = Counter()
	user_user_graph = {}
	user_hashtag_graph = {}
	hashtag_hashtag_graph = {}
	tweets = {}
	processed_lines = 0
	
	savefile = os.path.basename(targetfile)
	print('Processing file {} ({}).......'.format(savefile, multiprocessing.current_process())) 		
	with open(targetfile , 'r', encoding='utf-8') as f:
		#lines = [next(f) for x in range(100)]
		lines = f.readlines()
		print('lines to be processed:{}::{}'.format(savefile,len(lines)))
		for line in lines:
			try:
				status = json.loads(line)
				process_status(status, influencer_frequency_dist, 
								mentioned_frequency_dist, hashtag_frequency_dist, 
								url_frequency_dist,user_user_graph, user_hashtag_graph,
								hashtag_hashtag_graph, tweets)
				processed_lines += 1
			except:
				traceback.print_exc()
	print('lines processed:{}::{}'.format(savefile, processed_lines))

	# Output a bunch of files containing the data we just gathered
	print("Saving data {}".format(savefile))
	json_outputs = {"tweets.json": tweets,
					"urls.json": url_frequency_dist,
					"hashtags.json": hashtag_frequency_dist,
					"influencers.json": influencer_frequency_dist,
					"mentioned.json": mentioned_frequency_dist,
					"user_user_graph.json": user_user_graph,
					"user_hashtag_graph.json": user_hashtag_graph,
					"hashtag_hashtag_graph.json": hashtag_hashtag_graph}
	for name, dataset in json_outputs.items():
		filename = os.path.join(save_dir, '{}-{}'.format(savefile,name))
		save_json(dataset, filename)

# These files are created in a format that can be easily imported into Gephi
	csv_outputs = {"user_user_graph.csv": user_user_graph,
					"user_hashtag_graph.csv": user_hashtag_graph,
					"hashtag_hashtag_graph.csv": hashtag_hashtag_graph}
	for name, dataset in csv_outputs.items():
		filename = os.path.join(save_dir,'{}-{}'.format(savefile,name))
		save_csv(dataset, filename)

	text_outputs = {"hashtags.txt": hashtag_frequency_dist,
					"influencers.txt": influencer_frequency_dist,
					"mentioned.txt": mentioned_frequency_dist,
					"urls.txt": url_frequency_dist}
	for name, dataset in text_outputs.items():
		filename = os.path.join(save_dir, '{}-{}'.format(savefile,name))
		save_text(dataset, filename)

	return processed_lines

if __name__ == '__main__':

	processed_lines_all = 0
	target = '/tmp/twitter-political/*17-json'
	files = glob.glob(target)
	# for targetfile in files:
	# 	processed_lines_all += process_file(targetfile)
	# print('lines processed:{}'.format(processed_lines_all))
	num_cpu = multiprocessing.cpu_count() - 2 if multiprocessing.cpu_count() > 2 else 1
	print('num_cpu: {}'.format(num_cpu))
	#proccessed = p_map(process_file,files, num_cpus = num_cpu)
	with multiprocessing.Pool(processes=num_cpu) as p:    
		for res in tqdm(p.imap(process_file, files),total=len(files)):
			if res is not None:
				print('Number of lines proccessed:{}'.format(res))
				processed_lines_all += res

	print('Number of tweets proccessed:{}'.format(processed_lines_all))	