
'''
data processing: starting with reading filenames for loading
'''

from glob import glob 
from datetime import datetime
import numpy as np
import pandas as pd
import time

from processing_helpers import * 
from summarizing_helpers import *
from city_data_helpers import * 

# data_source = '/data/'
# data_dest = '/data/app_data/'

data_source = '/Users/kotaminegishi/big_data_training/python/dash_BLM/'
data_dest = '/Users/kotaminegishi/big_data_training/python/dash_BLM/'

def process_new_data(timestamp=''):

    '''
        Stage 1: process raw data into several datasets 

    '''

    if timestamp == '': 
        current_time = datetime.utcnow() + pd.DateOffset(hours=-6)
        current_time_s = current_time.strftime('%Y-%m-%d %H:%M:%S')
    else:
        current_time_s = timestamp

    current_time_s = pd.to_datetime(current_time_s)
    current_time_h = current_time_s.floor("h")


    # read file names
    files_1 =  keep_recent_files(glob(data_source + "BLM_past_original/*/*/*"), 
        current_time_s, file_type= '.json', days = 1, hours=7, no_newer=True, no_newer_h=7, prefix = 'BLM_tweet_')
    files_2 = keep_recent_files(glob(data_source + "BLM_past_retweet/*/*/*"),
        current_time_s, file_type= '.json', days = 1, hours=7, no_newer=True, no_newer_h=7, prefix = 'BLM_retweet_')


    # try:
    #     # read previous filenames
    #     existing_files_1 = pd.read_csv(data_dest + 'data_filenames/files_read_BLM_past_original.csv')
    #     existing_files_2 = pd.read_csv(data_dest + 'data_filenames/files_read_BLM_past_retweet.csv')

    #     # get new file names 
    #     new_files_1 = [file for file in files_1 if file.split('dash_BLM/')[1] not in np.array(existing_files_1.name)]
    #     new_files_2 = [file for file in files_2 if file.split('dash_BLM/')[1] not in np.array(existing_files_2.name)]
    # except:
    #read all files as new files
    new_files_1 = files_1
    new_files_2 = files_2

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
        except: 
            try: print('Going to process {} records of retweets.'.format(len(rt.df)))
            except: print('No new data to process at this time.')


    ori.assign_sentiments()
    ori.assign_emotions()
    ori.count_words()

    rt.assign_sentiments()
    rt.assign_emotions()
    rt.count_words()


    # load stats from cumulative retweet to match with new data
    ref_sentiments = pd.read_csv(data_dest + 
        'data_cumulative/retweet/2020_all_sentiments.csv')
    ref_emotions = pd.read_csv(data_dest + 
        'data_cumulative/retweet/2020_all_emotions.csv')
    ref_words = pd.read_json(data_dest + 
        'data_cumulative/retweet/2020_all_words.json', orient='records', lines=True)

    fix_datetime(ref_sentiments)
    fix_datetime(ref_emotions)
    fix_datetime(ref_words)
    fix_token_counter(ref_words)


    # combine new stat data and matched data from cumulative retweet data 
    if new_original:
        new_sentiments = merge_datasets(or_df = ori.df, 
                                         or_data = ori.df_sentiments, 
                                         ref_data = ref_sentiments)

        new_emotions = merge_datasets(or_df = ori.df, 
                                        or_data = ori.df_top_emotions, 
                                        ref_data = ref_emotions)

        new_words = merge_datasets(or_df = ori.df, 
                                         or_data = ori.df_words, 
                                         ref_data = ref_words)

        # store datetime as string
        new_words.created_at_h = new_words.created_at_h.astype(str)
        ori.df.created_at_h = ori.df.created_at_h.astype(str)

        # add new data into cumulative datasets
        time_as_filename = 'created_at_' + str(current_time_s).replace(" ","_")
        new_sentiments.to_csv(data_dest + 'data_cumulative/sentiments/' + time_as_filename + '.csv', index=False)
        new_emotions.to_csv(data_dest + 'data_cumulative/emotions/'+ time_as_filename + '.csv', index=False)
        new_words.to_json(data_dest + 'data_cumulative/words/'+ time_as_filename +'.json', orient='records', lines=True)
        ori.df.to_json(data_dest + 'data_cumulative/original/'+ time_as_filename +'.json', orient='records', lines=True)

        # correct datetime data type
        new_words.created_at_h = pd.to_datetime(new_words.created_at_h)
        ori.df.created_at_h = pd.to_datetime(ori.df.created_at_h)

        print('  Updated cumulative data: sentiments, emotions, words, and original.')



    cum_retweet = pd.read_json(data_dest + "data_cumulative/retweet/2020_all_retweets.json",
     lines=True, orient='records')
    fix_RT_id(cum_retweet)

    # overwrite retweet data
    if new_retweet: 
        cum_retweet = cum_retweet.append(rt.df)

        cum_retweet = cum_retweet.drop_duplicates(subset = 'RT_id')
        cum_retweet.created_at_h = cum_retweet.created_at_h.astype(str)
    
        cum_retweet.to_json(data_dest + "data_cumulative/retweet/2020_all_retweets.json",
            lines=True, orient='records')
        print('  Updated cumulative data: retweet.')

        file_loc = 'data_cumulative/retweet/'

        rt.df_words = rt.df_words.reset_index()
        rt.df_sentiments = rt.df_sentiments.reset_index()
        rt.df_top_emotions = rt.df_top_emotions.reset_index()

        ref_words = ref_words.append(rt.df_words)
        ref_words.created_at_h = ref_words.created_at_h.astype(str)
        ref_sentiments = ref_sentiments.append(rt.df_sentiments)
        ref_emotions = ref_emotions.append(rt.df_top_emotions)

        ref_words.append(rt.df_words).to_json(data_dest + file_loc + '2020_all_words.json', orient='records', lines=True)
        ref_sentiments.to_csv(data_dest + file_loc + '2020_all_sentiments.csv', index=False)
        ref_emotions.to_csv(data_dest + file_loc + '2020_all_emotions.csv', index=False) 
        print('  Updated cumulative data: retweet-words, sentiments, and emotions.')


    # # correct data types
    # fix_datetime(cum_retweet)
    # fix_datetime(ref_sentiments)
    # fix_datetime(ref_emotions)
    # fix_datetime(ref_words)
    # fix_token_counter(ref_words)


    try:
        # append new file names 
        if new_original:
            new_files_1s = [file.split('dash_BLM/')[1] for file in new_files_1]

            pd.DataFrame(new_files_1s, 
                     columns = {'name'}
                    ).to_csv(data_dest + 'data_filenames/files_read_BLM_past_original.csv', 
                             mode='a', header=False, index=False)

        if new_retweet: 
            new_files_2s = [file.split('dash_BLM/')[1] for file in new_files_2]

            pd.DataFrame(new_files_2s, 
                     columns = {'name'}
                    ).to_csv(data_dest + 'data_filenames/files_read_BLM_past_retweet.csv', 
                             mode='a', header=False, index=False)

    except:
        ## initial files
        if new_original:
            new_files_1s = [file.split('dash_BLM/')[1] for file in new_files_1]

            pd.DataFrame(new_files_1s, 
                     columns = {'name'}
                    ).to_csv(data_dest + 'data_filenames/files_read_BLM_past_original.csv', 
                             mode='w', header=True, index=False)

        if new_retweet: 
            new_files_2s = [file.split('dash_BLM/')[1] for file in new_files_2]

            pd.DataFrame(new_files_2s, 
                     columns = {'name'}
                    ).to_csv(data_dest + 'data_filenames/files_read_BLM_past_retweet.csv', 
                             mode='w', header=True, index=False)

    if new_original: del new_sentiments, new_emotions, new_words, ori, rt
    if new_retweet: del cum_retweet, ref_sentiments, ref_emotions, ref_words




    '''
        Stage 2: Process data into data_cumulative/city_date data 

    '''


    cities_all = ['all_v1']
    

    data_dest_files = data_dest + 'data_cumulative/'
    days_to_keep = 2 # files to read within x days
    days_to_process = 2 # original tweet ids to match within x days 

    # current_time = datetime.utcnow() + pd.DateOffset(hours=-6)
    # current_time_s = current_time.strftime('%Y-%m-%d %H:%M:%S')

    # current_time_s = pd.to_datetime(current_time_s)
    base_timestamp = current_time_s#.floor('h')
    #base_timestamp =  pd.to_datetime(datetime(2020,7,20))

    print('Going to process city_date data: ', base_timestamp)   


    files_retweet = [data_dest + 'data_cumulative/retweet/2020_all_retweets.json']


    files_all_original = keep_recent_files(
        glob(data_dest_files + 'original/*'), 
        base_timestamp, 
        file_type= '.json', 
        days = days_to_keep, no_newer=True)

    files_all_sentiments = keep_recent_files(
        glob(data_dest_files + 'sentiments/*'), 
        base_timestamp, 
        file_type= '.csv', 
        days = days_to_keep, no_newer=True)

    files_all_emotions = keep_recent_files(
        glob(data_dest_files + 'emotions/*'), 
        base_timestamp, 
        file_type= '.csv', 
        days = days_to_keep, no_newer=True)

    files_all_words = keep_recent_files(
        glob(data_dest_files + 'words/*'), 
        base_timestamp, 
        file_type= '.json', 
        days = days_to_keep, no_newer=True)

    # try:
    #     # read previous filenames
    #     files_existing_original = pd.read_csv(data_dest + 'data_filenames/files_read_original.csv')
    #     files_existing_sentiments = pd.read_csv(data_dest + 'data_filenames/files_read_sentiments.csv')
    #     files_existing_emotions = pd.read_csv(data_dest + 'data_filenames/files_read_emotions.csv')
    #     files_existing_words = pd.read_csv(data_dest + 'data_filenames/files_read_words.csv')

    #     # get new file names 
    #     files_original = [file for file in files_all_original if file.split('data_cumulative/')[1] not in np.array(files_existing_original.name)]
    #     files_sentiments = [file for file in files_all_sentiments if file.split('data_cumulative/')[1] not in np.array(files_existing_sentiments.name)]
    #     files_emotions = [file for file in files_all_emotions if file.split('data_cumulative/')[1] not in np.array(files_existing_emotions.name)]
    #     files_words = [file for file in files_all_words if file.split('data_cumulative/')[1] not in np.array(files_existing_words.name)]

    #     mode = 'a'
    #     header = False

    # except:
    # initialize filenames 
    files_original = files_all_original
    files_sentiments = files_all_sentiments
    files_emotions = files_all_emotions
    files_words = files_all_words
    mode = 'w'
    header = True


    '''
    process data for cities and cities_all
    '''

    print('Going to process the following retweet files:', files_retweet)

    df_retweet = pd.read_json(files_retweet[0], orient='records',lines=True)
    for c in cities_all:
        filename = data_dest + 'data_cumulative/city_date/' + c + '/retweet/2020_all_retweets.json'
        df_retweet.to_json(filename, orient='records',lines=True)
        print('updated ',  filename)

    print('Going to process the following original files:', files_original)
    
    original_files_by_city_date_json(files_original, cities_all, [], 
                                data_dest, verbose=True, city_type='all_v1')


    print('Going to process the following sentiments files:', files_sentiments)
    files_id_matched_by_city_date_json(
        files_sentiments, cities_all, data_dest, 'sentiments', 
        base_timestamp, process_days = days_to_process,
        file_type='.csv', verbose=True)


    print('Going to process the following emotions files:',  files_emotions)
    files_id_matched_by_city_date_json(
        files_emotions, cities_all, data_dest, 'emotions', 
        base_timestamp, process_days = days_to_process,
        file_type='.csv', verbose=True)


    print('Going to process the following words files:', files_words)
    files_id_matched_by_city_date_json(
        files_words, cities_all, data_dest, 'words', 
        base_timestamp, process_days = days_to_process,
        file_type='.json', verbose=True)

    
    # # update filenames for those that are read and processed
    # files_original_s = [file.split(data_dest_files)[1] for file in files_original]
    # files_sentiments_s = [file.split(data_dest_files)[1] for file in files_sentiments]
    # files_emotions_s = [file.split(data_dest_files)[1] for file in files_emotions]
    # files_words_s = [file.split(data_dest_files)[1] for file in files_words]


    # files_read_update(files_original_s, 
    #     data_dest + 'data_filenames/files_read_original', mode=mode, header =header)

    # files_read_update(files_sentiments_s, 
    #     data_dest + 'data_filenames/files_read_sentiments', mode=mode, header =header)

    # files_read_update(files_emotions_s, 
    #     data_dest + 'data_filenames/files_read_emotions', mode=mode, header =header)

    # files_read_update(files_words_s, 
    #     data_dest + 'data_filenames/files_read_words', mode=mode, header =header)


    update_current_data = True
    '''
        Stage 3: Pre-calculate statistics for data_current/city and data_cumulative/city_date/stat 
    '''
    # Execute the following when "update_current_data = True"
    # This section updates data for the unfiltered data using a small sample
    if update_current_data:
        print('\nUpdating current data files...')
        
        process_timestamp =  base_timestamp
        print('process_timestamp: ', process_timestamp)   
        current_datetime = process_timestamp.floor('h')
        
        # include potential repair candidates for the past hrs of the same date 
        all_stats_datetime = [str(current_datetime + pd.Timedelta(-h, unit='h')) for h in range(1, current_datetime.hour)]
        #all_stats_datetime = [str(current_datetime + pd.Timedelta(-h, unit='h')) for h in range(1,24)]

       # try:
        # read previous filenames
        existing_stats_datetime = pd.read_csv(data_dest + 'data_filenames/files_stats.csv')
        # get new file names 
        repair_stats_datetime = [datetime for datetime in all_stats_datetime if datetime not in list(existing_stats_datetime.datetime)]
        if len(repair_stats_datetime)>0: print('Repairing city_date stats for the following datetime: ', repair_stats_datetime)
        mode = 'a'
        header = False

        # except:
        #     # initialize filenames 
        #     repair_stats_datetime = all_stats_datetime
        #     mode = 'w'
        #     header = True


        stats_datetime = repair_stats_datetime.copy()
        stats_datetime.append(str(current_datetime))
        
        update_current_data_city(cities_all, data_dest, process_timestamp, repair_stats_datetime)

        # files_read_update(stats_datetime, 
        #     data_dest + 'data_filenames/files_stats', colname = 'datetime', mode=mode, header =header)
        # print('city_date stats updated for: ',  stats_datetime)





if __name__=="__main__":

    import nltk
    nltk.download('vader_lexicon') 
    nltk.download('stopwords')
    nltk.download('averaged_perceptron_tagger')
    nltk.download('wordnet')
    nltk.download('punkt')


    timestamps = [
    '2020-05-27 23:59', 
    '2020-05-30 23:59',
    '2020-06-03 23:59', 
    '2020-06-06 23:59', 
    '2020-06-10 23:59', 
    '2020-06-13 23:59', 
    '2020-06-17 23:59', 
    '2020-06-20 23:59', 
    '2020-06-24 23:59', 
    '2020-06-27 23:59', 
    ]

    for t in timestamps:
        process_new_data(timestamp=t)
   


