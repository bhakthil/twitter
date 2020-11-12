# This file contains code to aggregate data
import pandas as pd
from tqdm import tqdm
import glob
import dask.dataframe as dd
from datetime import datetime
import traceback
import os
import csv

#custom_date_parser = lambda x: datetime.strptime(x, "%a %b %d %H:%M:%S +0000 %Y")
custom_date_parser = lambda x: date_parser(x)


columns = ['id','created_at','text','source','tweet_type','source_tweet_id','retweeted_tweet_id',
           'quoted_tweet_id','quote_count','retweet_count','favorite_count','hashtags','most_unrolled_urls',
           'tweet_links','user_screen_name','user_id','user_mentions','user_followers_count',
           'user_following_count','user_listed_count','user_created_at','user_favourites_count','user_verified','user_statuses_count']

data_type_dict = {'id':str,'created_at':object,'text':str,'source':str,'tweet_type':str,'source_tweet_id':str,'retweeted_tweet_id':str,
           'quoted_tweet_id':str,'quote_count':object,'retweet_count':object,'favorite_count':object,'hashtags':str,'most_unrolled_urls':str,
           'tweet_links':str,'user_screen_name':str,'user_id':str,'user_mentions':str,'user_followers_count':object,
           'user_following_count':object,'user_listed_count':object,'user_created_at':object,'user_favourites_count':object,
           'user_verified':object,'user_statuses_count':object}
target = './output/data/*-json.csv'
files = glob.glob(target)

processed_files = []
files_to_process = [file for file in files]
unroccessed_files = [os.path.basename(file) for file in files]   
  
def date_parser(str_date):
    try:
        #'Fri Dec 21 22:53:46 +0000 2012'
        custom_date = datetime.strptime(str_date, "%a %b %d %H:%M:%S +0000 %Y") if str_date else None
        #print(custom_date_parser)
        return custom_date
    except:
        return None

def process_file(file):
    base_name = os.path.basename(file)
    try:
        print('[[{}]] Starting to load....'.format(base_name))
        df = pd.read_csv(file,sep='\t', encoding='iso-8859-1', warn_bad_lines=True, error_bad_lines=False)
        # df = pd.read_csv(file,sep='\t', encoding='iso-8859-1', names=columns, warn_bad_lines=True, error_bad_lines=False, dtype=data_type_dict)
        print('[[{}]] Ended loading....{}'.format(base_name, df.shape))
        processed_files.append(base_name)
        unroccessed_files.remove(base_name)
        return df
    except Exception as e:
        print('[[{}]] Failed loading....{}'.format(base_name, str(e)))
        traceback.print_exc()    


def process_file_in_loop(file, wf_good, wf_bad):
    base_name = os.path.basename(file)
    good =0
    bad = 0
    print('[[{}]] Starting to load....'.format(base_name))
    try:
        with open(file, 'r') as r: 
            next(r)                   # SKIP HEADERS
            reader = csv.reader(r, delimiter='\t')
            try:
                for row in reader:
                    if (row and len(row)==24):
                        wf_good.writerow(row)
                        good += 1
                    else:
                        wf_bad.writerow(row)
                        bad += 1
                        print('[[{}]] error line @{}'.format(base_name, reader.line_num))
                        
            except csv.Error as e:
                print('[[{}]] Failed line @{}'.format(base_name, reader.line_num))
                traceback.print_exc() 
        
                    
        print('[[{}]] Ended loading....valid:{} invalid:{}'.format(base_name, good, bad))
        processed_files.append(base_name)
        unroccessed_files.remove(base_name)
       
    except Exception as e:
        print('[[{}]] Failed loading....{}'.format(base_name, str(e)))
        traceback.print_exc()
    


def do_combine_in_loop():
    print('Concatanating files...')
    start_time = datetime.now()
    try:
        with open('./output/data/combined_data.csv', 'w', newline='\n') as fout:
            wf = csv.writer(fout, delimiter='\t')
            ## add header row
            wf.writerow(columns)
            with open('./output/data/combined_data_invalid.csv', 'w', newline='\n') as fout_invalid:
                wf_invalid = csv.writer(fout_invalid, delimiter='\t')
                ## add header row
                wf_invalid.writerow(columns)
                [process_file_in_loop(file, wf, wf_invalid) for file in tqdm(files_to_process)]
    except:
        traceback.print_exc()
    total_time = datetime.now() - start_time
    print('total time for concatanating data:{}'.format(total_time))
    print('proccessed files {}:{}'.format(len(processed_files),'\n'.join(processed_files)))
    print('unprocessed files {}:{}'.format(len(unroccessed_files),'\n'.join(unroccessed_files)))

        
def do_combine():

    print('Concatanating dataframes...')

    # try:
    #     start_time = datetime.now()
    #     df = dd.read_csv(target, encoding='iso-8859-1', names=columns, warn_bad_lines=True, error_bad_lines=False, dtype=data_type_dict, sep='\t')
    #     # dd.to_csv(df,'./output/data/combined_data.gzip', single_file=True,columns=columns, index=False, compression="gzip", sep='\t')
    #     dd.to_parquet(df,'./output/data/parquet/', write_index=False )
    #     total_time = datetime.now() - start_time
    #     print('total time for concatanating data:{}'.format(total_time))
    # except:
    #     traceback.print_exc()
    start_time = datetime.now()
    try:

        frames = [ process_file(file) for file in tqdm(files_to_process)]
        print('concatenating.....')
        merged_df = pd.concat(frames, ignore_index=True)
        #merged_df.to_csv('./output/data/combined_data.gzip',columns=columns, index=False, compression="gzip")
        merged_df.to_csv('./output/data/combined_data.gzip', index=False, compression="gzip")
        print('merged_df.info():{}'.format(merged_df.info()))
    except:
        traceback.print_exc()
    total_time = datetime.now() - start_time
    print('total time for concatanating data:{}'.format(total_time))
    print('proccessed files {}:{}'.format(len(processed_files),'\n'.join(processed_files)))
    print('unprocessed files {}:{}'.format(len(unroccessed_files),'\n'.join(unroccessed_files)))

if __name__ == "__main__":
    #do_combine()
    do_combine_in_loop()
    
    #date_parsing('Fri Dec 21 22:53:46 +0000 2012')