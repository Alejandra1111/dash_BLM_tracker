import pandas as pd

from utilities import time_now_pandas, append_a_content_to_file, \
    append_and_drop_duplicates, append_a_list_to_csv_file, df_to_json
from filename_utils import * 
from newTweetData import *

from globals import data_source, data_dest, current_time, current_time_str, \
    to_json_args, read_json_args 


def process_new_data():
    print(f'\nProcessing new tweet data:{"-"*20}\n')

    append_a_content_to_file(
        content = '\n'+current_time_str,
        file = data_dest + 'data_processing_log/processing_begin.csv')

    files_original, files_retweet = get_new_data_filenames(verbose=True)

    ori = NewTweetData(files_original.new_filenames, 'id', current_time)
    rt  = NewTweetData(files_retweet.new_filenames, 'RT_id', current_time)

    ori.assign_sentiments_and_wordcounts()
    rt.assign_sentiments_and_wordcounts()

    message_processing_record_counts(ori, rt)

    retweets, rt_words, rt_sentiments, rt_emotions = load_and_append_retweet_data(rt)

    new_sentiments = append_matching_rt_data(ori.df, ori.df_sentiments, rt_sentiments)
    new_emotions = append_matching_rt_data(ori.df, ori.df_top_emotions, rt_emotions)
    new_words = append_matching_rt_data(ori.df, ori.df_words, rt_words)

    save_new_data(new_sentiments, new_emotions, new_words, ori)

    files_original.append_new_filenames(
        alt_filename = f'{data_dest}data_filenames/files_read_BLM_tweet_original.csv')

    files_retweet.append_new_filenames(
        alt_filename = f'{data_dest}data_filenames/files_read_BLM_tweet_retweet.csv')



def get_new_data_filenames(verbose=True):
    # original-type tweet data files:
    files_original = FilenameGatherer(new_file_location = data_source, 
                                      new_file_prefix = "BLM_tweet_original_*/*/*/*/*", 
                                      existing_file_location = data_dest,
                                      existing_filenames_file= 'data_filenames/files_read_BLM_tweet_original.csv')
    files_original.register_filename_reader(read_existing_filenames_csv)
    files_original.gather_filenames()
    if verbose:
        print('original tweet data files:')
        for file in files_original.new_filenames: print(file.split(data_source)[1])

    # retweet-type tweet data files:
    files_retweet = FilenameGatherer(new_file_location = data_source, 
                                      new_file_prefix = "BLM_tweet_retweet_*/*/*/*/*", 
                                      existing_file_location = data_dest,
                                      existing_filenames_file= 'data_filenames/files_read_BLM_tweet_retweet.csv')
    files_retweet.register_filename_reader(read_existing_filenames_csv)
    files_retweet.gather_filenames()
    if verbose:
        print('retweet data files:')
        for file in files_retweet.new_filenames: print(file.split(data_source)[1])

    return files_original, files_retweet


def load_and_append_retweet_data(rt):
    # load stats from cumulative retweet to match with new data
    retweets = pd.read_json(data_dest + "data_cumulative/retweet/2020_all_retweets.json", **read_json_args)
    rt_sentiments = pd.read_csv(data_dest + 
        'data_cumulative/retweet/2020_all_sentiments.csv')
    rt_emotions = pd.read_csv(data_dest + 
        'data_cumulative/retweet/2020_all_emotions.csv')
    rt_words = pd.read_json(data_dest + 
        'data_cumulative/retweet/2020_all_words.json',  **read_json_args)

    if len(rt.df):
        retweets = append_and_drop_duplicates(retweets, rt.df, 'RT_id')
        rt_sentiments = append_and_drop_duplicates(rt_sentiments, rt.df_sentiments, 'RT_id')
        rt_emotions = append_and_drop_duplicates(rt_emotions, rt.df_top_emotions, 'RT_id')
        rt_words = append_and_drop_duplicates(rt_words, rt.df_words, 'RT_id')

        file_rt_loc = 'data_cumulative/retweet/'
        df_to_json(retweets, f'{data_dest}{file_rt_loc}2020_all_retweets.json',
            vars_to_str=['created_at', 'created_at_h'], **to_json_args)
        df_to_json(rt_words, f'{data_dest}{file_rt_loc}2020_all_words.json',
             vars_to_str='created_at_h', **to_json_args)
        rt_sentiments.to_csv(f'{data_dest}{file_rt_loc}2020_all_sentiments.csv', index=False) 
        rt_emotions.to_csv(f'{data_dest}{file_rt_loc}2020_all_emotions.csv', index=False) 
        print('Updated retweet data: retweet, retweet-words, sentiments, and emotions.')

    return retweets, rt_words, rt_sentiments, rt_emotions


def save_new_data(new_sentiments, new_emotions, new_words, ori):
    # save new data 
    time_as_filename = 'created_at_' + current_time_str.replace(" ","_")
    new_sentiments.to_csv(f'{data_dest}data_cumulative/sentiments/{time_as_filename}.csv', index=False)
    new_emotions.to_csv(f'{data_dest}data_cumulative/emotions/{time_as_filename}.csv', index=False)
    df_to_json(new_words, f'{data_dest}data_cumulative/words/{time_as_filename}.json', 
        vars_to_str ='created_at_h', **to_json_args)
    df_to_json(ori.df, f'{data_dest}data_cumulative/original/{time_as_filename}.json',
        vars_to_str =['created_at', 'created_at_h'], **to_json_args)
    print('Added new tweet data: sentiments, emotions, words, and original.')



def message_processing_record_counts(ori, rt):
    try:
        print('Going to process {} records of original tweets and {} records of retweets.'.format(len(ori.df), len(rt.df)))
        
    except:
        try:  print('Going to process {} records of original tweets.'.format(len(ori.df)))
        except: 
            try: print('Going to process {} records of retweets.'.format(len(rt.df)))
            except: print('No new data to process at this time.')




