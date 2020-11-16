
import requests
import os
import json
from searchtweets import collect_results, ResultStream, gen_request_parameters, load_credentials
from searchtweets.utils import write_result_stream
import fileinput
import csv
import pandas as pd
import time

conversation_id = "1316019730407329798"
MAX_TWEETS=5000
RESULTS_PER_CALL=100
OUTPUT_FILE='./twitter_data_sample_{}.json'
INPUT_FILE='./tweets_sample.csv'
WAIT=4
# Tweet fields are adjustable.
# Options include:
# attachments, author_id, context_annotations,
# conversation_id, created_at, entities, geo, id,
# in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
# possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
# source, text, and withheld
# (is:retweet OR is:quote)
twitter_query = "conversation_id:{} (is:retweet OR is:quote)"
tweet_fields = "conversation_id,author_id,created_at,id,public_metrics,referenced_tweets,text,source"
expansions = "referenced_tweets.id,author_id"
user_fields='created_at,public_metrics,verified,name,username'
def gen_request_query(id):
    query = gen_request_parameters(query=twitter_query.format(id),
                                tweet_fields=tweet_fields,
                                expansions=expansions,
                                user_fields=user_fields, 
                                results_per_call=RESULTS_PER_CALL,start_time="2020-10-24T00:00")
    
    return query

def main():
    search_args = load_credentials(filename="keys.yaml", yaml_key="search_tweets_v2", env_overwrite=False)
    df = pd.read_csv(INPUT_FILE)
    ids = df['id'].tolist()
    tweetdict = {}
    for tweet_id in ids:
        query = gen_request_query(tweet_id)
        
        print(query)
        
        # rs = ResultStream(request_parameters=query,max_results=MAX_TWEETS, **search_args)
        # rs.max_requests=MAX_TWEETS
        # rs.max_tweets=RESULTS_PER_CALL
        # stream = write_result_stream(rs,filename_prefix='twitter_data_{}'.format(str(tweet_id)))
        tweets = collect_results(query, max_tweets=MAX_TWEETS, result_stream_args=search_args) # change this if you need to
        tweet_list =[]
        tweet_list.extend(tweets)
        #tweetdict[tweet_id]=tweet_list
        with open(OUTPUT_FILE.format(str(tweet_id)), 'w', encoding='utf-8') as f:
            json.dump(tweet_list, f, indent=2)
                
            
        
    print('done')        # for tweet in tweets:
            #     json.dump(tweet, f, indent=4)
            #     #json.dump(tweet, f)

                
            #     print(tweet)
        
            #f.write()
    # rs = ResultStream(request_parameters=query, max_results=1000, max_pages=1, **search_args)
    # rs.max_tweets=100
    # rs.max_requests=1000
    # print(rs)    

    # tweet_list = list(rs.stream())
    # print(tweet_list)
'''
    for tweet in tweet_list:
        try:
            for key, val in tweet.items():
                print(key, val)
            #tweet = Tweet(line,do_format_validation=False)
        except Exception as e:
            print(e)
        #print(tweet.created_at_string, tweet.all_text)
'''
if __name__ == "__main__":
    main()
