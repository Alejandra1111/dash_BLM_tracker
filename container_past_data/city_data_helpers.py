       
import numpy as np
import pandas as pd
from datetime import datetime
import json
import os
from glob import glob 
import itertools

import time

from processing_helpers import * 
from summarizing_helpers import *

#data_dest = '/data/app_data/'
data_source = '/Users/kotaminegishi/big_data_training/python/dash_BLM/'
data_dest = '/Users/kotaminegishi/big_data_training/python/dash_BLM/'


def get_columns_json(file):
    chunk1 = pd.read_json(file, chunksize=1, orient='records', lines=True)
    for d in chunk1:
        data1 = d.iloc[0]
        break
    return list(data1.keys())

def get_columns_csv(file):
    chunk1 = pd.read_csv(file, chunksize=1)
    return list(chunk1.read(1).keys())


def load_null_df(data_path):
    
    null_cum_sentiments = pd.DataFrame(columns = get_columns_csv(
                     glob(data_path + 'data_cumulative/sentiments/*')[0]))
    
    null_cum_emotions = pd.DataFrame(columns = get_columns_csv(
                     glob(data_path + 'data_cumulative/emotions/*')[0]))
    
    null_cum_words = pd.DataFrame(columns = get_columns_json(
                     glob(data_path + 'data_cumulative/words/*')[0]))
    
    null_cum_original = pd.DataFrame(columns = get_columns_json(
                     glob(data_path + 'data_cumulative/original/*')[0]))
    
    null_cum_retweet = pd.DataFrame(columns = get_columns_json(
                     data_path + 'data_cumulative/retweet/2020_all_retweets.json'))
    
    return null_cum_sentiments, null_cum_emotions, null_cum_words, null_cum_original, null_cum_retweet



def load_null_df2(data_path):
    
    null_stat_sentiments = pd.DataFrame(columns = get_columns_csv(
                     glob(data_path + 'data_current/city/all_v1/stat_sentiments*')[0]))
    
    null_stat_emotions = pd.DataFrame(columns = get_columns_csv(
                     glob(data_path + 'data_current/city/all_v1/stat_emotions*')[0]))
    
    null_stat_words = pd.DataFrame(columns = get_columns_json(
                     glob(data_path + 'data_current/city/all_v1/stat_words*')[0]))
    
    null_top_tweets = pd.DataFrame(columns = get_columns_csv(
                     glob(data_path + 'data_current/city/all_v1/top_tweets*')[0]))
    
    null_top_users = pd.DataFrame(columns = get_columns_csv(
                     glob(data_path + 'data_current/city/all_v1/top_users*')[0]))

    return null_stat_sentiments, null_stat_emotions, null_stat_words, null_top_tweets, null_top_users


# load null dfs 
null_cum_sentiments, null_cum_emotions, null_cum_words, null_cum_original, null_cum_retweet =load_null_df(data_dest)
null_stat_sentiments, null_stat_emotions, null_stat_words, null_top_tweets, null_top_users = load_null_df2(data_dest)


def get_city_data(city, data_path, base_timestamp, hour_stats_only=False):
    cum_data_path = data_path + "data_cumulative/city_date/" + city
    curr_data_path = data_path + "data_current/city/" + city
    
    if hour_stats_only:
        stat_sentiments = null_stat_sentiments
        stat_emotions = null_stat_emotions
    else:
        # load recent cumulative data
        print('  Loading cumulative data: sentiments and emotions...')
        files_sentiments = keep_recent_files(glob(cum_data_path + "/sentiments/*"),
                            base_timestamp=base_timestamp, prefix='records_',
                            file_type = '.csv', days=14, no_newer=True, no_newer_h=7) 
        if len(files_sentiments)>0: 
            cum_sentiments = tw_data_files_to_df_csv(files_sentiments)
            cum_sentiments = cum_sentiments.drop_duplicates(subset = 'id')
            fix_datetime(cum_sentiments)
            stat_sentiments = calc_stat_sentiments(cum_sentiments)
        else:
            stat_sentiments = null_stat_sentiments
            
        files_emotions = keep_recent_files(glob(cum_data_path + "/emotions/*"),
                            base_timestamp=base_timestamp, prefix='records_',
                            file_type = '.csv', days=14, no_newer=True, no_newer_h=7)
        if len(files_emotions)>0:
            cum_emotions = tw_data_files_to_df_csv(files_emotions)
            cum_emotions = cum_emotions.drop_duplicates(subset = 'id')
            fix_datetime(cum_emotions)    
            stat_emotions = calc_stat_emotions(cum_emotions)
        else:
            stat_emotions = null_stat_emotions

    print('  Loading cumulative data: words...')
    files_words = keep_recent_files(glob(cum_data_path + "/words/*"),
                                    base_timestamp=base_timestamp, prefix='records_',
                                    file_type = '.json', days=7, no_newer=True, no_newer_h=7) 

    if len(files_words)>0:
        cum_words = tw_data_files_to_df_json(files_words, lines=True)
        fix_datetime(cum_words)
        fix_token_counter(cum_words)
    else:
        cum_words = null_cum_words

    print('  Loading cumulative data: original tweets and retweets...')   
    # load recent cumulative data     
    files_original = keep_recent_files(glob(cum_data_path + "/original/*"),
        base_timestamp = base_timestamp, days=7, no_newer=True, no_newer_h=7,
        prefix='records_', file_type = '.json')
    if len(files_original)>0:
        cum_original = tw_data_files_to_df_json(files_original, lines=True)
        fix_datetime(cum_original)        
        fix_RT_id(cum_original)
    else:
        cum_original = null_cum_original

    files_retweet = cum_data_path + "/retweet/2020_all_retweets.json"
    try: 
        cum_retweet = pd.read_json(files_retweet, orient='records', lines=True)
        fix_datetime(cum_retweet)
        fix_RT_id(cum_retweet)
    except:
        cum_retweet = null_cum_retweet

    latest_datatime = cum_original.created_at_h.max()
    time_now =  min([latest_datatime, base_timestamp])
    print(time_now)

    cum_data = cumulative_data(cum_ori = cum_original, 
                              cum_rt = cum_retweet,
                              cum_words = cum_words,
                              now = time_now,
                              hour_stats_only = hour_stats_only
                              )

    cum_data.add_words_subsets()
    cum_data.add_tweet_subsets()
    cum_data.add_user_subsets()

    return stat_sentiments, stat_emotions, cum_data.stat_words, cum_data.top_tweets, cum_data.top_users
    

def consolidate_stats(stat_df, hour_df):
    if len(stat_df)==0: return hour_df
    tmp = str(stat_df.subset[0]).split('hour_') 
    if len(tmp) > 1:
        hour_x = 'hour_' + str(tmp[1])
        hour_df = hour_df[hour_df.subset != hour_x]
    return stat_df.append(hour_df)



def update_current_data_city(cities, data_path, base_timestamp, repair_stats_datetime):
    for city in cities:
        print('\nUpdating current city data files for ' + city)

        stat_sentiments, stat_emotions, stat_words, top_tweets, top_users = get_city_data(city, data_path, base_timestamp)

        curr_data_path = data_path + "data_current/city/" + city

        # update current data: recent cumulative files
        stat_sentiments.to_csv(curr_data_path + '/stat_sentiments.csv', index = False)
        stat_emotions.to_csv(curr_data_path +  '/stat_emotions.csv', index = False)
        print('  Updated current data: stat_sentiments and stat_emotions.')

        stat_words.to_json(curr_data_path + '/stat_words.json', orient='records', lines=True)
        top_users.to_csv(curr_data_path + '/top_users.csv', index=False)
        top_tweets.to_csv(curr_data_path + '/top_tweets.csv', index=False)
        print('  Updated current city data: stat_words, top_users, and top_tweets.')


        print('Updating city stats:')
        if int(str(base_timestamp)[11:13])==0:
            # at processing time hour=0, it will process the data for hour=23 of the previous day 
            datetime_d = str(pd.to_datetime(str(base_timestamp)[:10]) - pd.Timedelta(1, unit='d'))[:10]
        else: 
            datetime_d = str(base_timestamp)[:10]
        hour_words = pd.DataFrame()
        hour_tweets = pd.DataFrame()
        hour_users = pd.DataFrame()

        city_date_stats_path = data_path + "data_cumulative/city_date/" + city + "/stats"
        filename = city_date_stats_path + '/stats_'+ datetime_d.replace(" ",'_') + '.json'
        new_file = glob(filename)==[]

        if not new_file:
            # read existing hourly stats from stat_words, top_tweets, top_users     
            with open(filename) as file:
                stats0 = json.load(file)

            stat_words0 = pd.read_json(stats0['stat_words'], orient='split')
            top_tweets0 = pd.read_json(stats0['top_tweets'], orient='split')
            top_users0 = pd.read_json(stats0['top_users'], orient='split')

            if len(stat_words0)>0:
                hour_words = hour_words.append(
                    stat_words0[stat_words0.subset.apply(lambda x: x.startswith('hour_'))])
            if len(top_tweets0)>0:
                hour_tweets = hour_tweets.append(
                    top_tweets0[top_tweets0.subset.apply(lambda x: x.startswith('hour_'))])
            if len(top_users0)>0: 
                hour_users = hour_users.append(
                    top_users0[top_users0.subset.apply(lambda x: x.startswith('hour_'))])

        if len(repair_stats_datetime)>0:
            for datetime in repair_stats_datetime:
                print('repairing hourly data for:', datetime)
                tmp_sent, stat_emo, tmp_words, tmp_tweets, tmp_users = get_city_data(city, data_path, pd.to_datetime(datetime), hour_stats_only=True)

                hour_words = hour_words.append(tmp_words)
                hour_tweets = hour_tweets.append(tmp_tweets)
                hour_users = hour_users.append(tmp_users)
        
        print(stat_words)
        print(hour_words)

        if len(hour_words)>0: stat_words = consolidate_stats(stat_words, hour_words)
        if len(hour_tweets)>0: top_tweets = consolidate_stats(top_tweets, hour_tweets)
        if len(hour_users)>0: top_users = consolidate_stats(top_users, hour_users)
        print(stat_words)
        print(top_tweets)
        print(top_users)

        stats = {
         'stat_sentiments': stat_sentiments.to_json(orient='split'),
         'stat_emotions': stat_emotions.to_json(orient='split'),
         'stat_words': stat_words.to_json(orient='split'),
         'top_tweets': top_tweets.to_json(orient='split'),
         'top_users': top_users.to_json(orient='split'),
         'timestamp': str(base_timestamp)
        }

        with open(filename, 'w') as file:
            json.dump(stats, file)



        
 
# def fix_datetime(df, timevar='created_at_h'):
#     df[timevar] = pd.to_datetime(df[timevar])

# def fix_token_counter(df):
#     df.token_counter = df.token_counter.apply(lambda x: Counter(x))  

# def fix_RT_id(df):
#     df.RT_id = df.RT_id.astype(str) 


# def convert_floats(df, float_dtype='float32'):
#     floats = df.select_dtypes(include=['float64']).columns.tolist()
#     df[floats] = df[floats].astype(float_dtype)
#     return df

# def tw_data_files_to_df_csv(files):
#     '''append and concat data files into a pandas.DataFrame'''
#     df = []
#     [ df.append(pd.read_csv(file)) for file in files ]
#     df = pd.concat(df, ignore_index=True)
#     return df


# def tw_data_files_to_df_json(files, lines=False):
#     '''append and concat data files into a pandas.DataFrame'''
#     df = []
#     [ df.append(pd.read_json(file, orient='records', lines=lines)) for file in files ]
#     df = pd.concat(df, ignore_index=True)
#     return df



# def keep_recent_files(files, base_timestamp, file_type= '.json', days = 14, no_newer=False,
#                       prefix = 'created_at_'):
#     timestamps = [pd.Timestamp(file.split(prefix,1)[1]
#                                .replace(file_type,'').replace('_',' ')) for file in files ]
#     if no_newer: 
#         keep_idx1 = [(base_timestamp - timestamp <= pd.Timedelta(days, unit='d')) & 
#                      (base_timestamp - timestamp >= pd.Timedelta(0, unit='d')) for timestamp in timestamps]
#     else: 
#         keep_idx1 = [base_timestamp - timestamp <= pd.Timedelta(days, unit='d') for timestamp in timestamps]
#     return(list(itertools.compress(files,keep_idx1)))




def clean_token_cityname(list_tokens):
    tokens = [(token.replace('.','').replace(',','').replace('!','')
               .replace('?','').replace('#','')) for token in list_tokens] 
    return tokens


def mark_tokens_contain_keyword(df, keyword):
    # returns an index indicating whether variable 'tokens' contains keyword
    return df.tokens.apply(lambda x: keyword.lower() in clean_token_cityname(x))

def mark_tokens_contain_keywords(df, keywords):
    idx = [mark_tokens_contain_keyword(df, keyword) for keyword in keywords]
    return pd.DataFrame(idx).agg(max).astype(bool)
    
def mark_tokens_contain_keyword_jointly(df, keywords):
    # returns an index indicating whether variable 'tokens' contains keyword
    idx = [mark_tokens_contain_keyword(df, keyword) for keyword in keywords]
    return pd.DataFrame(idx).agg(min).astype(bool) 
    
def mark_var_in_valuelist(df, var, valuelist):
    # returns an index indicating whether variable var is in valuelist
    return df[var].apply(lambda x: x in valuelist)

def get_columns_json(file):
    chunk1 = pd.read_json(file, chunksize=1, orient='records', lines=True)
    for d in chunk1:
        data1 = d.iloc[0]
        break
    return list(data1.keys())

def get_columns_csv(file):
    chunk1 = pd.read_csv(file, chunksize=1)
    return list(chunk1.read(1).keys())

def df_vars_convert_to_str(df, vars):
    for var in vars:
        df[var] = df[var].astype(str)
        

def tw_data_files_to_df_json_filter(files, filter_word, lines=True, float_dtype=None, verbose=False):
    '''append and concat filtered data into a pandas.DataFrame'''
    if type(filter_word) != list: raise ValueError("filter_word must be a list")

    df = []
    for file in files:
        if verbose: print('loading ' + file)  
        if file==files[0]:
            columns = get_columns_json(file)
            df_null = pd.DataFrame(columns=columns)
            
        df_file = pd.read_json(file, orient='records', lines=lines)
        if (len(filter_word) >1): idx = mark_tokens_contain_keywords(df_file, filter_word)
        else: idx = mark_tokens_contain_keyword(df_file, filter_word[0])
        df_file_filtered = df_file[idx]
        if len(df_file_filtered)>0:
            df.append(df_file_filtered)
    
    if len(df)==0: return df_null
    df = pd.concat(df, ignore_index=True)
    if float_dtype is None: return df
    return convert_floats(df, float_dtype)

def tw_data_files_to_df_json_match_id(files, varname_id, list_ids,
                                      lines=True, float_dtype=None, verbose=False):
    '''append and concat filtered data into a pandas.DataFrame'''
    if type(list_ids) != list: raise ValueError("list_ids must be a list")

    df = []
    for file in files:
        if verbose: print('loading ' + file)  
        if file==files[0]:
            columns = get_columns_json(file)
            df_null = pd.DataFrame(columns=columns)
            
        df_file = pd.read_json(file, orient='records', lines=lines)
        idx = mark_var_in_valuelist(df_file, varname_id, list_ids)
        df_file_filtered = df_file[idx]
        if len(df_file_filtered)>0:
            df.append(df_file_filtered)
    
    if len(df)==0: return df_null
    df = pd.concat(df, ignore_index=True)
    if float_dtype is None: return df
    return convert_floats(df, float_dtype)


def mark_var_contain_filterwords(df, varname, filterwords):
    if type(filterwords) != list: raise ValueError("filterwords must be a list")
    idx = {}
    for word in filterwords:
        if type(word)==str:
            idx[str(word)] = df[varname].apply(lambda x: word.lower() in clean_token_cityname(x))
        elif type(word)==list:
            # assess whether all components of 'word' are jointly present 
            loc_idx = [df[varname].apply(lambda x: w.lower() in clean_token_cityname(x)) for w in word]
            idx[str(word)] = pd.DataFrame(loc_idx).agg(min).astype(bool)
        else: raise ValueError('each item in filterwords must be str or list')
        # assess whether any component of 'filterwords' are present 
    return pd.DataFrame(idx).agg(max, axis=1).astype(bool)  


def retweet_files_by_city_json(files, cities, city_filterwords, data_path,
                               lines=True, float_dtype='float16', verbose=False):
    city_df = {}

    for file in files:
        if verbose: print('loading ' + file)  
        if file==files[0]:
            columns = get_columns_json(file)
            df_null = pd.DataFrame(columns=columns)
            for city in cities:
                city_df[city] = []
        
        df_file = pd.read_json(file, orient='records', lines=lines)
        df_vars_convert_to_str(df_file, ['RT_id','user_id','created_at','created_at_h'])
        convert_floats(df_file, float_dtype)
        
        for city in cities:
            filter_word = city_filterwords[city]    
            idx = mark_var_contain_filterwords(df_file, 'tokens', filter_word)
            if sum(idx)>0: city_df[city].append(df_file[idx])
    
    for city in cities:
        if len(city_df[city])==0: city_data = df_null
        else: city_data = pd.concat(city_df[city], ignore_index=True)
        filename = 'data_cumulative/city_date/' + city + '/retweet/2020_all_retweets' + '.json'
        city_data.to_json(data_path + filename, 
                          orient='records', lines=lines)
        print('updated: ', filename)


def get_unique_dates(df, varname):
    tmp = pd.to_datetime(df[varname]).dt.floor('d')
    dates = tmp.unique()
    dates_str = [str(date)[:10] for date in dates]
    return dates, dates_str

def filter_df_by_date(df, varname, date, var_as_string=True):
    tmp_df = df
    varname_d = varname + '_d'
    tmp_df[varname_d] = pd.to_datetime(tmp_df[varname]).dt.floor('d')
    filtered_df = tmp_df[tmp_df[varname_d] == pd.to_datetime(date)].drop(columns = [varname_d])
    if var_as_string: filtered_df[varname] = filtered_df[varname].astype(str)
    return filtered_df

def append_to_json(filename, df, lines=True):
    df0 = pd.read_json(filename, orient='records', lines=lines)
    return df0.append(df)



def original_files_by_city_date_json(files, cities, city_filterwords, data_path,
                               lines=True, float_dtype='float16', verbose=False,
                                    city_type='city', sample_frac = .05):
    
    if len(files)==0: return None
    if city_type not in ['city','all','all_v1']: raise ValueError('city_type must be "city" or "all".')
    print('Found ' + str(len(files)) + ' files to process')

    city_df = {}
    city_RT_ids = {}
    
    
    for city in cities:
        # retrieve relevant RT_id to match 
        filename = 'data_cumulative/city_date/' + city + '/retweet/2020_all_retweets' + '.json'
        RT_id = pd.read_json(data_path + filename, 
                             orient='records', lines=True).RT_id.astype(str)
        city_RT_ids[city] = list(RT_id)
    
    for file in files:
        if verbose: print('loading ' + file)  
        if file==files[0]:
            columns = get_columns_json(file)
            df_null = pd.DataFrame(columns=columns)
            for city in cities:
                city_df[city] = []
        
        df_file = pd.read_json(file, orient='records', lines=lines)
        df_vars_convert_to_str(df_file, ['id','RT_id','created_at','created_at_h'])
        convert_floats(df_file, float_dtype)
        
        for city in cities:
            if city_type=='city':
                if verbose: print('processing data for ' + city)  
                filter_word = city_filterwords[city]
                # idx1: 'tokens' containing filter_word
                idx1 = mark_var_contain_filterwords(df_file, 'tokens', filter_word)
                # idx2: relevant retweet's that are matched  
                idx2 = mark_var_in_valuelist(df_file, 'RT_id', city_RT_ids[city])
                # idx: either idx1 or idx2 being True
                idx = pd.DataFrame(data={'idx1':idx1, 'idx2': idx2}).agg(max, axis=1)
                print(sum(idx1),sum(idx2), sum(idx))
                if sum(idx)>0: city_df[city].append(df_file[idx])
            elif city_type=='all':
                city_df[city].append(df_file.sample(frac=sample_frac, replace=False))
            elif city_type=='all_v1':
                city_df[city].append(df_file)
            
    for city in cities:
        if len(city_df[city])==0: city_data = df_null
        else: city_data = pd.concat(city_df[city], ignore_index=True)
        dates, dates_str = get_unique_dates(city_data,'created_at_h')
        for date in dates_str:
            if verbose: print('processing date of ' + date)  
            df_date = filter_df_by_date(city_data, 'created_at_h', date)
            filename = 'data_cumulative/city_date/' + city + '/original/records_'+ date + '.json'
            new_file = glob(data_path + filename)==[]
            if new_file:
                df_date.to_json(data_path + filename, 
                              orient='records', lines=lines)
                print('created: ', filename)
            else:
                df_date = append_to_json(data_path + filename, df_date)
                df_date.to_json(data_path + filename, 
                              orient='records', lines=lines)
                print('appended: ', filename)


def keep_by_matched_id(df, list_id, varname='id'):
    return (df.set_index(varname)
            .join(pd.DataFrame(data={varname: list_id}).set_index(varname), how='inner')
            .reset_index()
            )


def files_id_matched_by_city_date_json(
    files, cities, data_path, folder, process_datetime, process_days = 5,
    file_type ='.json', float_dtype='float16', lines=True, verbose=False):

    '''
    Looks for recent files in /city_date/[city]/original/*, extract relevant ids,
    generate data matched with those ids by city, and create data files  
    '''
    if len(files)==0: return None
    print('Found ' + str(len(files)) + ' files to process')

    if file_type not in ['.json', '.csv'] :
        raise ValueError('file_type must be either json or csv')
            
    city_df = {}
    city_ids = {}
    for city in cities:
        files_city_original = keep_recent_files(
            glob(data_path + "data_cumulative/city_date/" + city  + "/original/*"),
            prefix = 'records_', file_type= '.json', 
            base_timestamp = process_datetime, days=process_days,
            no_newer=True)
        tmp_ids = []
        for file in files_city_original:
            # retrieve relevant id to match
            if verbose: print('reading ids from ' + file)
            ids = pd.read_json(file, orient='records', lines=True).id.astype(str)
            tmp_ids.append(ids)
        city_ids[city] = list(pd.concat(tmp_ids, ignore_index=True)) if len(tmp_ids)>0 else []
    
    for file in files:
        if verbose: print('loading ' + file)  
        if file==files[0]:
            columns = get_columns_json(file) if file_type =='.json' else get_columns_csv(file)
            df_null = pd.DataFrame(columns=columns)
            for city in cities:
                city_df[city] = []

        if file_type =='.json': 
            df_file = pd.read_json(file, orient='records', lines=lines)
        elif file_type =='.csv': 
            df_file = pd.read_csv(file)

        df_vars_convert_to_str(df_file, ['id','created_at_h'])
        convert_floats(df_file, float_dtype)

        for city in cities:
            # tmp_df: relevant original tweet's that are matched  
            tmp_df = keep_by_matched_id(df_file, city_ids[city], varname='id')
            if verbose: print('matched data for ' + city + ': ' + str(len(tmp_df)) + ' records')  
            if len(tmp_df)>0: city_df[city].append(tmp_df)
    
    for city in cities:
        if len(city_df[city])==0: 
            city_data = df_null
        else: 
            city_data = pd.concat(city_df[city], ignore_index=True)
        dates, dates_str = get_unique_dates(city_data, 'created_at_h')
        for date in dates_str:
            if verbose: print('processing date of ' + date)  
            df_date = filter_df_by_date(city_data, 'created_at_h', date)
            filename = 'data_cumulative/city_date/' + city + '/' + folder + '/records_'+ date + file_type
            new_file = glob(data_path + filename)==[]
            if file_type =='.json': 
                if not new_file:
                    df_date = append_to_json(data_path + filename, df_date)
                df_date.to_json(data_path + filename, 
                              orient='records', lines=lines)
            if file_type =='.csv':
                mode = 'w' if new_file else 'a'
                header = True if new_file else False
                df_date.to_csv(data_path + filename, index=False, header=header, mode=mode)
            if new_file: print('created: ', filename)
            else: print('appended: ', filename)
            


def files_read_update(files, filename, mode='a', 
        colname = 'name', header=False, index=False):
        pd.DataFrame(files, 
                 columns = {colname}
                ).to_csv(filename + '.csv', 
                         mode=mode, header=header, index=index)


