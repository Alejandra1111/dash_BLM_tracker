
from glob import glob 
from datetime import datetime
import numpy as np
import pandas as pd
import time

from processing_helpers import * 
from summarizing_helpers import *

nltk.download('vader_lexicon') 
nltk.download('stopwords')
nltk.download('averaged_perceptron_tagger')
nltk.download('wordnet')
nltk.download('punkt')

data_source = '/data/'
data_dest = '/data/app_data/'


# read file names
files_1 = glob(data_source + "BLM_tweet_original_*/*/*/*/*")
files_2 = glob(data_source + "BLM_tweet_retweet_*/*/*/*/*")


current_time = datetime.utcnow() + pd.DateOffset(hours=-6)
current_time_s = current_time.strftime('%Y-%m-%d %H:%M:%S')
current_time_s

processing_begin_logs = pd.read_csv(data_dest + 'data_processing_log/processing_begin.csv')
processing_begin_logs.timestamp = pd.to_datetime(processing_begin_logs.timestamp)
processing_begin_logs_h = [time.floor("h") for time in processing_begin_logs.timestamp]

current_time_s = pd.to_datetime(current_time_s)
current_time_h = current_time_s.floor("h")

# flag for updating current data if it is not processed in the same clock hour
update_current_data = current_time_h not in processing_begin_logs_h
# ! overwrite for testing
update_current_data = True

print('update_current_data:' ,  update_current_data)




# initial file creation
# pd.DataFrame([current_time_s], columns = {'timestamp'}).to_csv('data_processing_log/processing_begin.csv', index=False)

# append to file log                                                 line_terminator='\n')
with open(data_dest + 'data_processing_log/processing_begin.csv','a') as fd:
    fd.write('\n'+str(current_time_s))


# read previous filenames
existing_files_1 = pd.read_csv(data_dest + 'data_filenames/files_read_BLM_tweet_original.csv')
existing_files_2 = pd.read_csv(data_dest + 'data_filenames/files_read_BLM_tweet_retweet.csv')

# get new file names 
new_files_1 = [file for file in files_1 if file.split(data_source)[1] not in np.array(existing_files_1.name)]
new_files_2 = [file for file in files_2 if file.split(data_source)[1] not in np.array(existing_files_2.name)]


print('original tweet files:')
[ print(file) for file in new_files_1]
print('retweet files:')
[ print(file) for file in new_files_2]


new_original = len(new_files_1)>0
new_retweet = len(new_files_2)>0


# get new data 
ori = new_tw_data(new_files_1, type='original')
rt  = new_tw_data(new_files_2, type='retweet')


try:
    print('Going to process {} records of original tweets and {} records of retweets.'.format(len(ori.df), len(rt.df)))
    
except:
    try:  print('Going to process {} records of original tweets.'.format(len(ori.df)))
    except: print('No new data to process at this time.')


ori.assign_sentiments()
ori.assign_emotions()
ori.count_words()

bgn_timespan = ori.df.created_at.min() if new_original else current_time_s

# try:
#if new_original:
    # load recent cumulative data
print('  Loading cumulative data: sentiments and emotions...')

files_sentiments = keep_recent_files(glob(data_dest + "data_cumulative/sentiments/*"),
                    base_timestamp=bgn_timespan,
                    file_type = '.csv', days=14) 
cum_sentiments = tw_data_files_to_df_csv2(files_sentiments, frac=0.05, float_dtype='float16')
cum_sentiments = cum_sentiments.drop_duplicates(subset = 'id')

files_emotions = keep_recent_files(glob(data_dest + "data_cumulative/emotions/*"),
                    base_timestamp=bgn_timespan,
                    file_type = '.csv', days=14)
cum_emotions = tw_data_files_to_df_csv2(files_emotions, frac=0.05, float_dtype='float16')
cum_emotions = cum_emotions.drop_duplicates(subset = 'id')

# correct data types
cum_sentiments = fix_datetime(cum_sentiments)
cum_emotions = fix_datetime(cum_emotions)


#    print('  Loading cumulative data: words...')

files_words = keep_recent_files(glob(data_dest + "data_cumulative/words/*"),
                                base_timestamp=bgn_timespan,
                                file_type = '.json', days=7) 
cum_words = tw_data_files_to_df_json3(files_words, lines=True, frac=0.05, float_dtype='float16')
cum_words = fix_datetime(cum_words)
cum_words = fix_token_counter(cum_words)
    
# except Exception as e:
#    print(e.__doc__)


#if new_original:
new_sentiments = merge_datasets(or_df = ori.df, 
                                 or_data = ori.df_sentiments, 
                                 ref_data = cum_sentiments)

new_emotions = merge_datasets(or_df = ori.df, 
                                or_data = ori.df_top_emotions, 
                                ref_data = cum_emotions)

new_words = merge_datasets(or_df = ori.df, 
                                 or_data = ori.df_words, 
                                 ref_data = cum_words)


# append new data in memory
cum_sentiments = cum_sentiments.append(new_sentiments)
cum_emotions = cum_emotions.append(new_emotions)
cum_words = cum_words.append(new_words)


# update current data to load in dash app: these datasets are cumulative
stat_sentiments = calc_stat_sentiments(cum_sentiments)
stat_sentiments.to_csv(data_dest + 'data_current/stat_sentiments.csv', index = False)

stat_emotions = calc_stat_emotions(cum_emotions)
stat_emotions.to_csv(data_dest + 'data_current/stat_emotions.csv', index = False)
print('  Updated current data: stat_sentiments and stat_emotions.')



## initial files
#pd.DataFrame(new_files_1, columns = {'name'}).to_csv('data_filenames/files_read_BLM_tweet_original.csv', index=False)
#pd.DataFrame(new_files_2, columns = {'name'}).to_csv('data_filenames/files_read_BLM_tweet_retweet.csv', index=False)

# append new file names 
#if new_original:
new_files_1s = [file.split(data_source)[1] for file in new_files_1]

pd.DataFrame(new_files_1s, 
         columns = {'name'}
        ).to_csv(data_dest + 'data_filenames/files_read_BLM_tweet_original.csv', 
                 mode='a', header=False, index=False)
#if new_retweet: 
new_files_2s = [file.split(data_source)[1] for file in new_files_2]

pd.DataFrame(new_files_2s, 
         columns = {'name'}
        ).to_csv(data_dest + 'data_filenames/files_read_BLM_tweet_retweet.csv', 
                 mode='a', header=False, index=False)


#if new_original: 
# store datetime as string
new_words.created_at_h = new_words.created_at_h.astype(str)
ori.df.created_at_h = ori.df.created_at_h.astype(str)

# add new data in cumulative datasets
time_as_filename = 'created_at_' + str(current_time_s).replace(" ","_")
new_sentiments.to_csv(data_dest + 'data_cumulative/sentiments/' + time_as_filename + '.csv', index=False)
new_emotions.to_csv(data_dest + 'data_cumulative/emotions/'+ time_as_filename + '.csv', index=False)
new_words.to_json(data_dest + 'data_cumulative/words/'+ time_as_filename +'.json', orient='records', lines=True)
ori.df.to_json(data_dest + 'data_cumulative/original/'+ time_as_filename +'.json', orient='records', lines=True)

# correct datetime data type
new_words.created_at_h = pd.to_datetime(new_words.created_at_h)
ori.df.created_at_h = pd.to_datetime(ori.df.created_at_h)

print('  Updated cumulative data: sentiments, emotions, words, and original.')

#if new_retweet: 
rt.df.created_at_h = rt.df.created_at_h.astype(str)
rt.df.to_json(data_dest + 'data_cumulative/retweet/'+ time_as_filename +'.json', orient='records', lines=True)
rt.df.created_at_h = pd.to_datetime(rt.df.created_at_h)
print('  Updated cumulative data: retweet.')








'''
    Execute the following when "update_current_data = True"
'''
#if update_current_data:
print('\nUpdating current data files...')

base_timestamp = current_time_s
# base_timestamp = datetime(2020, 7, 9, 10, 00, 00)

#  try: 
print('  Loading cumulative data: original and retweet...')
# load recent cumulative data     
files_original = keep_recent_files(glob(data_dest + "data_cumulative/original/*"),
    base_timestamp = base_timestamp, days=7)
cum_original = tw_data_files_to_df_json3(files_original, lines=True, frac=0.05, float_dtype='float16')


files_retweet = keep_recent_files(glob(data_dest + "data_cumulative/retweet/*"),
    base_timestamp = base_timestamp, days=365)
cum_retweet = tw_data_files_to_df_json3(files_retweet, lines=True, float_dtype='float16')



# correct data types
cum_original = fix_datetime(cum_original)
cum_retweet = fix_datetime(cum_retweet)

cum_original = fix_RT_id(cum_original)
cum_retweet = fix_RT_id(cum_retweet)

# append new data in memory
if new_original: cum_original = cum_original.append(ori.df)
if new_retweet: cum_retweet = cum_retweet.append(rt.df)

latest_datatime = cum_original.created_at_h.max()
time_now =  min([latest_datatime, base_timestamp])


cum_data = cumulative_data(cum_ori = cum_original, 
                          cum_rt = cum_retweet,
                          cum_words = cum_words,
                          now = time_now
                          )

cum_data.add_words_subsets()
cum_data.add_tweet_subsets()
cum_data.add_user_subsets()


# update current data to load in dash app: these datasets mostly use data within the past two weeks (except retweet data)

cum_data.stat_words.to_json(data_dest + 'data_current/stat_words.json', orient='records', lines=True)
cum_data.stat_words.head()

cum_data.top_users.to_csv(data_dest + 'data_current/top_users.csv', index=False)
cum_data.top_users.head()

cum_data.top_tweets.to_csv(data_dest + 'data_current/top_tweets.csv', index=False)
cum_data.top_tweets.head()

print('  Updated current data: stat_words, top_users, and top_tweets.')

