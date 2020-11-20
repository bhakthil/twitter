import networkx as nx
import pandas as pd
import dateutil
from tqdm import tqdm
import random
import csv

columns = ['id','created_at','text','source','tweet_type','source_tweet_id','retweeted_tweet_id',
           'quoted_tweet_id','quote_count','retweet_count','favorite_count','hashtags','most_unrolled_urls',
           'tweet_links','user_screen_name','user_id','user_mentions','user_followers_count',
           'user_following_count','user_listed_count','user_created_at','user_favourites_count','user_verified','user_statuses_count']
# This method will return if the user_id, is friends with target_user_id
def is_friend(user_id, target_user_id):
    return random.choice([True,False])

# This method will return if the given user is valid or not
def is_valid_user(user_id):
    return False

edge_list = []

if __name__ == '__main__':
    #main
    df = pd.read_csv('./network_data/cache-0-json-network_data.txt', index_col=False, parse_dates=['created_at'])
    #df['created_at'] = df['created_at'].apply(dateutil.parser.parse, dayfirst=True)
    #print(df.info())
    
    df_groupby = df.groupby('source_tweet_id', as_index=False)
    missing_sources = []
    source_ids = [key for key, _ in df_groupby]
    for source_id in tqdm(source_ids):
        grouped_dataframe = df_groupby.get_group(source_id).reset_index(drop=True)
        grouped_dataframe = grouped_dataframe.sort_values('created_at', ascending=False)
        source_tweet = df[df['id']==source_id].head(1)
        if(source_tweet.size==0):
            missing_sources.append(source_id)
        else:
            source_user_id = source_tweet['user_id'].values[0]
            source_user_screen_name = source_tweet['user_screen_name'].values[0]
            source_created_at = source_tweet['created_at'].values[0]
            #print(user0_id,user0_screen_name )
            friend_found = False
            size = len(grouped_dataframe)
            for i in range(0, size):
                current_user_id = grouped_dataframe.iloc[i]['user_id']
                created_at = grouped_dataframe.iloc[i]['created_at']
                #print(current_user_id)
                friend_found = is_friend(current_user_id,source_user_id)
                if friend_found:    # no need to iterate if current user follows the original twitter user
                    time_lapse = source_created_at - created_at
                    edge_list.append('{} {} {{delay:{}}}'.format(current_user_id,source_user_id, time_lapse.total_seconds()))
                else:   # check if current user follows previous users. if frienship is not found, iterate untill found
                    for j in range(i+1, size):
                        prev_user_id = grouped_dataframe.iloc[j]['user_id']
                        prev_created_at = grouped_dataframe.iloc[i]['created_at']
                        friend_found = is_friend(current_user_id,prev_user_id)
                        if friend_found:
                            time_lapse = created_at - prev_created_at
                            edge_list.append('{} {} {{delay:{}}}'.format(current_user_id,source_user_id, time_lapse.total_seconds()))
                            break
                # No friends found. so, will create an edge between the current user and source user anyway
                if (friend_found is False):
                    time_lapse = source_created_at - created_at
                    lapse = time_lapse.total_seconds()
                    edge_list.append('{} {} {{delay:{}}}'.format(current_user_id,source_user_id, time_lapse.total_seconds()))
    print(len(missing_sources))
    
    with open('./network_data/cache-0-json.edgelist', 'w') as f: 
        f.writelines('%s\n' % edge for edge in edge_list)
    