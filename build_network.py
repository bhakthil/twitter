import networkx as nx
import pandas as pd
import dateutil



if __name__ == '__main__':
    #main
    df = pd.read_csv('./output/data/cache-0-json.csv', sep='\t')
    #df['created_at'] = df['created_at'].apply(dateutil.parser.parse, dayfirst=True)
    print(df.info())
    
    
    df_grouby = df.groupby('source_tweet_id')['source_tweet_id'].agg(['count']).query('count >100')
    df_grouby['count'] = df_grouby[['id']].transform(count)