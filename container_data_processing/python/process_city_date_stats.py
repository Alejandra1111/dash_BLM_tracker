import json
from glob import glob
from pandas import Timestamp

from utilities import subset_var_startswith, get_a_set_str_startswith, \
    get_hour_value_from_timestamps
from localDataAccess import *
from datedFiles import * 
from dataLoader import *
from tweetRetweetData import *
from logfile_utils import *

from globals import data_dest, cities, cities_all, \
    read_json_args, json_split, data_types, stat_days_short, stat_days_long


def process_city_date_stats():
    from globals import current_time, current_time_str

    print(f'\nProcessing city date stats:{"-"*20}\n')
    data_dest_cd = f'{data_dest}data_cumulative/city_date/'

    stats_datetime_candidates = get_same_day_hours_up_to_timestamp(current_time)

    logfile_stats = LogFileUtilityCSV(f'{data_dest}data_processing_log/', 'log_stats.csv', varname='datetime')     
    logfile_stats.add_candidate_records(stats_datetime_candidates)
    logfile_stats.read_existing_records()
    logfile_stats.get_new_records()

    process_hours = get_hour_value_from_timestamps(logfile_stats.new_records)

    for city in cities + cities_all:
        create_or_update_city_date_stats(city, data_dest_cd, current_time, process_hours, verbose=True)

    logfile_stats.append_new_records(f'{data_dest}data_processing_log/log_stats.csv')
    print('city_date stats updated for: ', logfile_stats.new_records)


def create_or_update_city_date_stats(city, data_path, current_time, process_hours=[], verbose=True):    
    if verbose: print(f'\n\n{"-"*15} {city} {"-"*15}:')

    stat_sentiments, stat_emotions, stat_words, top_tweets, top_users \
        = get_city_date_stats(city, current_time, data_path, process_hours)

    city_date_stats_path = f'{data_path}{city}/stats'
    date = str(current_time)[:10]
    filename = f'{city_date_stats_path}/stats_{date.replace(" ","_")}.json'

    if check_for_combining_previously_obtained_hour_stats(process_hours, filename):
        hour_words, hour_tweets, hour_users = get_existing_hourly_stats(filename) 

        stat_words = consolidate_stats(stat_words, hour_words)
        top_tweets = consolidate_stats(top_tweets, hour_tweets)
        top_users = consolidate_stats(top_users, hour_users)
    
    if verbose:
        print('\nstat_words\n', stat_words)
        print('\ntop_tweets\n',top_tweets)
        print('\ntop_users\n',top_users)

    stats = bundle_stats_json(stat_sentiments, stat_emotions, stat_words, top_tweets, top_users, **json_split)
    save_stats_json(stats, filename)

def get_same_day_hours_up_to_timestamp(timestamp):
    timestamp = pd.to_datetime(timestamp).floor('h')
    hours = [str(timestamp + pd.Timedelta(-h, unit='h')) for h in range(0, timestamp.hour+1)]
    return hours[::-1]

def check_for_combining_previously_obtained_hour_stats(process_hours, filename):
    new_file = glob(filename)==[]
    return 0 < len(process_hours) < 24 and not new_file

def get_existing_hourly_stats(filename):       
    with open(filename) as file:
        stats0 = json.load(file)

    stat_words0, top_tweets0, top_users0 \
     = [pd.read_json(stats0[stat], **json_split) for stat in ['stat_words', 'top_tweets', 'top_users']]
    
    hour_words = subset_var_startswith(stat_words0, 'subset', 'hour_')
    hour_tweets = subset_var_startswith(top_tweets0, 'subset', 'hour_')
    hour_users = subset_var_startswith(top_users0, 'subset', 'hour_')
    return hour_words, hour_tweets, hour_users 

def consolidate_stats(df_stat, df_hour):
    if len(df_stat)==0: return df_hour
    if len(df_hour)==0: return df_stat
    hours_exist = get_a_set_str_startswith(df_stat, 'subset', 'hour_')
    idx = [x not in hours_exist for x in list(df_hour.subset)] 
    return df_stat.append(df_hour[idx], ignore_index=True)


def get_city_date_stats(city, datetime, path, process_hours=[]):
    pd_datetime = Timestamp(datetime)
    path_city = f'{path}{city}/'
    db_access = LocalDataAccess(path_city)

    folders = ('original','retweet','sentiments','emotions','words')
    files_dict = db_access.get_files_dict_for_folders(folders)
    dated_files= make_dated_files_from_files_dict_over_folders(files_dict, folders)

    loader = DataLoader(db_access)
    filename_filter_short = DatedFilenameFilter(pd_datetime, days = stat_days_short, no_newer=True)
    filename_filter_long = DatedFilenameFilter(pd_datetime, days = stat_days_long, no_newer=True)  

    data_original = loader.load(dated_files.original, filename_filter_short, **read_json_args, **data_types)
    data_retweet = DataLoaderJson(**read_json_args).load_data_from_single_file(db_access, dated_files.retweet.files[0])
    data_words = loader.load(dated_files.words, filename_filter_short, **read_json_args, **data_types)
    data_sentiments = loader.load(dated_files.sentiments, filename_filter_long, **data_types)
    data_emotions = loader.load(dated_files.emotions, filename_filter_long, **data_types)

    stat_sentiments = calc_stat_sentiments(data_sentiments)
    stat_emotions = calc_stat_emotions(data_emotions)
    tw_rtw_data = TweetRetweetData(data_original, data_retweet, data_words, pd_datetime, process_hours)
    stat_words, top_tweets, top_users = (getattr(tw_rtw_data, x) for x in ('stat_words', 'top_tweets', 'top_users'))
    return stat_sentiments, stat_emotions, stat_words, top_tweets, top_users


def make_dated_files_from_files_dict_over_folders(files_dict, folders):
    dated_files = PlaceHolder()
    for folder in folders:
        if folder=='retweet': 
            setattr(dated_files, folder, 
                DatedDataFiles(files_dict.get(folder), 
                    id_varname = 'RT_id', date_prefix=''))
        else: 
            setattr(dated_files, folder, 
                DatedDataFiles(files_dict.get(folder), 
                    id_varname = 'id', date_prefix='records_'))
    return dated_files


def bundle_stats_json(stat_sentiments, stat_emotions, stat_words, top_tweets, top_users, **kwargs):
    stats = {
     'stat_sentiments': stat_sentiments.to_json(**kwargs),
     'stat_emotions': stat_emotions.to_json(**kwargs),
     'stat_words': stat_words.to_json(**kwargs),
     'top_tweets': top_tweets.to_json(**kwargs),
     'top_users': top_users.to_json(**kwargs)
    }
    return stats

def save_stats_json(stats, filename):
    with open(filename, 'w') as file:
        json.dump(stats, file)


