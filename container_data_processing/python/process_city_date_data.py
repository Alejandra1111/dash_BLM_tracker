import pandas as pd
from glob import glob 

from utilities import PlaceHolder
from filename_utils import *
from dataProcessorByCityDate import *

from globals import current_time, current_time_str, data_dest,   \
         cities, city_filterwords, cities_all, days_to_keep, days_to_process

def process_city_date_data():
    print(f'\nProcessing city date data:{"-"*20}\n')

    datatypes = ['original', 'sentiments', 'emotions', 'words']
    all_files = get_new_data_filenames(datatypes)
    files_original, files_sentiments, files_emotions, files_words = [getattr(all_files, datatype) for datatype in datatypes ]
    files_retweet = [f'{data_dest}data_cumulative/retweet/2020_all_retweets.json']

    # use different 'city_df_filters' for cities and cities_all when processing retweet and original data
    # the order of the execution matters: process retweet first, then original, and then any of the sentiments, emotions, and words data 
    process_retweet_data(files_retweet)
    process_original_data(files_original)

    for data_type, file_type in zip(['sentiments', 'emotions', 'words'], ['csv','csv','json']):

        dp = DataProcessorByCityDate(
            getattr(all_files, data_type).new_filenames, cities + cities_all, city_filterwords, data_path=data_dest,
            data_type = data_type, file_type= file_type, 
            process_datetime = current_time, process_days = days_to_process)

        dp.register_city_df_filter(city_df_filter_by_id)
        dp.process_data()

    for files in [files_original, files_sentiments, files_emotions, files_words]:
        files.append_new_filenames()


def get_new_data_filenames(datatypes):
    files_retweet = [f'{data_dest}data_cumulative/retweet/2020_all_retweets.json']

    all_files = PlaceHolder() 

    for datatype in datatypes:

        files = FilenameGatherer(new_file_location =  f'{data_dest}data_cumulative/', 
                                  new_file_prefix = f'{datatype}/created_at_*', 
                                  existing_file_location = data_dest,
                                  existing_filenames_file= f'data_filenames/files_read_{datatype}.csv')
        files.register_filename_reader(read_existing_filenames_csv)
        files.register_filename_filter(filter_by_keeping_files_within_x_days_old, 
                                                current_time, days_to_keep, 
                                                date_prefix = 'created_at_', date_suffix ='.')
        files.gather_filenames()
        setattr(all_files, datatype, files)
    return all_files


def process_retweet_data(files_retweet):
    # process cities data
    dp_retweet_cities = DataProcessorByCityDate(
        files_retweet, cities, city_filterwords, data_path=data_dest,
        data_type = 'retweet', file_type='json')
    dp_retweet_cities.register_city_df_filter(city_df_filter_by_filterwords)
    dp_retweet_cities.process_data()

    # process cities_all data
    dp_retweet_cities_all = DataProcessorByCityDate(
        files_retweet, cities_all, city_filterwords, data_path=data_dest,
        data_type = 'retweet', file_type='json')
    dp_retweet_cities_all.register_city_df_filter(city_df_filter_by_RT_id)
    dp_retweet_cities_all.process_data()


def process_original_data(files_original):
    # process cities data
    dp_original_cities = DataProcessorByCityDate(
        files_original.new_filenames, cities, city_filterwords, data_path=data_dest,
        data_type = 'original', file_type='json')
    dp_original_cities.register_city_df_filter(city_df_filter_by_filterwords_or_RT_id)
    dp_original_cities.process_data()

    # process cities_all data
    dp_original_cities_all = DataProcessorByCityDate(
        files_original.new_filenames, cities_all, city_filterwords, data_path=data_dest,
        data_type = 'original', file_type='json', sample_frac=0.02)
    dp_original_cities_all.register_city_df_filter(city_df_filter_by_sample)
    dp_original_cities_all.process_data()



