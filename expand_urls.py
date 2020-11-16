from os import sep
import urllib as ulib
import mimetypes
import urllib as ulib
from urllib.parse import urlparse
from requests.exceptions import Timeout
import urlexpander
import pandas as pd
import csv 


## Hold memor cache of the links to avoid unnecessary network calls

COUNT_TOP_URLS = 75

def expand(valid_links_cache, expanded_urls, tweet_links):
    expanded_list = []
    #print('Expanding urls :: {} , {}'.format(expanded_urls,tweet_links))
    #urls = row['most_unrolled_urls']
    if isinstance(expanded_urls, str):
        links = expanded_urls.split('|') if(expanded_urls) else [] 
        for link in links:
            link = link.replace('\n','').strip()
            if(link in valid_links_cache.keys() and link not in expanded_list):
                expanded_list.append(valid_links_cache[link])
    else:
        print('Invalid expanded_urls :{}'.format(expanded_urls))
        
    if isinstance(tweet_links, str):
        links = tweet_links.split('|') if(tweet_links) else [] 
        for link in links:
            link = link.replace('\n','').strip()
            if(link in valid_links_cache.keys() and link not in expanded_list):
                expanded_list.append(valid_links_cache[link])
    else:
        print('Invalid tweet_links :{}'.format(tweet_links))    
    

    expanded_list = list(set(expanded_list))
    return '|'.join(expanded_list)

def generate_expanded_urls():
    valid_links_cache = {}
    with open('./output/cache-0-json-urls.txt', 'r', encoding='utf-8') as f:
        ct = 0
        lines = f.readlines() # [next(f) for x in range(COUNT_TOP_URLS)]
        while len(valid_links_cache) < COUNT_TOP_URLS:
            line = lines[ct]
            if(line):
                link = line.split('\t')
                url = link[1].replace('\n','').strip()
                parsed_url = urlparse(url)
                print('Hitting.....{}'.format(url))
                if(parsed_url.netloc != 'twitpic.com'):
                    try:
                        u = ulib.request.urlopen(url, timeout=10)
                        link_type = u.headers['Content-Type'] 
                        if(link_type and 'image' not in link_type):
                            valid_links_cache[url] = u.geturl()
                    except Exception as e:
                        print('Error :: {}:{}'.format(link, str(e)))
                    print('Done.....{}'.format(url))
            ct +=1
                
    with open('./network_data/cache-0-json-expanded-urls.txt', 'w') as f:
        f.write('{},{}\n'.format('short_link','valid_link'))
        for short_link, valid_link in valid_links_cache.items():
            f.write('{},{}\n'.format(short_link,valid_link))
    return valid_links_cache    
    #df_links = pd.DataFrame(valid_links_cache, index=[0])
    #df_links.to_csv('./network_data/cache-0-json-expanded-urls.txt', encoding='utf-8')        

def extarct_users(df):
   # Extract all the unique users from screen names
    list_users = df.user_screen_name.unique().tolist()
    with open('./network_data/cache-0-json-network_user.txt', 'w') as f: 
        for user in list_users:
            f.write('{}\n'.format(user)) 

if __name__ == "__main__":
    
    #generate_expanded_urls()
    
    df_list = []
    df = pd.read_csv('./output/data/cache-0-json.csv', sep='\t')
    valid_links_cache = pd.read_csv('./network_data/cache-0-json-expanded-urls.txt', encoding='utf-8', index_col=0, header=1, squeeze=True).to_dict()

# filter df for each url    
    for search_str in valid_links_cache.keys():
        search_str = search_str.replace('\n','').strip() 
        filtered_df = df[(df['most_unrolled_urls'].notnull() & df['most_unrolled_urls'].str.contains(search_str)) | \
                 (df['tweet_links'].notnull() & df['tweet_links'].str.contains(search_str))]
        #print(filtered_df.info())
        #filtered_df['expanded_links'] = filtered_df.apply(lambda x : expand(x['most_unrolled_urls']),axis=1)
        df_list.append(filtered_df)
        
    df_all = pd.concat(df_list)
    
    # expand the urls and add them to the df
    df_all['expanded_links'] = df_all.apply(lambda x : expand(valid_links_cache, x['most_unrolled_urls'], x['tweet_links']),axis=1)
    print(df_all.info())
    print(df_all.head(10))
    
    #Extract users
    #extarct_users(df_all)
    
    #Save data
    df_all = df_all.reset_index(drop=True)
    df_all.to_csv('./network_data/cache-0-json-network_data.txt', encoding='utf-8')
    
     
    
    
