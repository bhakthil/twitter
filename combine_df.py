# This file contains code to aggregate data
import pandas as pd
from tqdm import tqdm
import glob
#import dask.dataframe as dd
from datetime import datetime

#custom_date_parser = lambda x: datetime.strptime(x, "%a %b %d %H:%M:%S +0000 %Y")
custom_date_parser = lambda x: date_parser(x)

columns = ['id','created_at','text','source','tweet_type','source_tweet_id','retweeted_tweet_id',
           'quoted_tweet_id','quote_count','retweet_count','favorite_count','hashtags','most_unrolled_urls',
           'tweet_links','user_screen_name','user_id','user_mentions','user_followers_count',
           'user_following_count','user_listed_count','user_created_at','user_favourites_count','user_verified','user_statuses_count']

def date_parser(str_date):
    try:
        #'Fri Dec 21 22:53:46 +0000 2012'
        custom_date = datetime.strptime(str_date, "%a %b %d %H:%M:%S +0000 %Y") if str_date else None
        #print(custom_date_parser)
        return custom_date
    except:
        return None
    
def do_combine():
    target = './output/data/large-17-json.csv'
    data_type_dict = {'id':str,'created_at':object,'text':str,'source':str,'tweet_type':str,'source_tweet_id':str,'retweeted_tweet_id':str,
           'quoted_tweet_id':str,'quote_count':object,'retweet_count':object,'favorite_count':object,'hashtags':str,'most_unrolled_urls':str,
           'tweet_links':str,'user_screen_name':str,'user_id':str,'user_mentions':str,'user_followers_count':object,
           'user_following_count':object,'user_listed_count':object,'user_created_at':object,'user_favourites_count':object,
           'user_verified':object,'user_statuses_count':object}

    df = pd.read_csv(target,index_col=False, encoding='iso-8859-1', names=columns, 
                        warn_bad_lines=True, error_bad_lines=False, dtype=data_type_dict, parse_dates=['created_at','user_created_at'])
    print(df.info())
    print(df.tail(10))
    # df = dd.read_csv(target,
    #                  names=columns,
    #                  #parse_dates=['created_at','user_created_at'], 
    #                  dtype=object, 
    #                  #date_parser=custom_date_parser, 
    #                  verbose=True)
    # dd.to_csv(df,'./output/data/combined_data.csv', single_file=True)
    

if __name__ == "__main__":
    do_combine()
    
    #date_parsing('Fri Dec 21 22:53:46 +0000 2012')