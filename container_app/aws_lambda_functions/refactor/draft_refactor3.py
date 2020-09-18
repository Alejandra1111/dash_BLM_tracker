
import sys
sys.path.append('python')

import pandas as pd

from utilities import *
from s3DataAccess import *
from dataLoader import *
from tweetRetweetData import *



#def lambda_handler(event, context):
def lambda_handler(event):
    city = event['city']
    date = event['date']
    filter_keyword = event['filter_keyword']

    pd_date = pd.Timestamp(date)
    path = f'app_data/data_cumulative/city_date/{city}/' 
    folders = ('original','retweet','sentiments','emotions','words','wordindex')

    s3access = S3DataAccess(bucket_name, s3_client, s3_resource)

    files = {}
    for folder in folders:
        files[folder] = s3access.get_files(f'{path}{folder}/')

    dated_files = PlaceHolder()
    for folder in folders:
        if folder=='retweet': 
            setattr(dated_files, folder, 
                DatedDataFiles(files[folder], ignore_files_ends_with='.DS_Store', 
                    id_varname = 'RT_id', date_prefix=''))
        elif folder=='wordindex': 
            setattr(dated_files, folder, 
                DatedDataFiles(files[folder], ignore_files_ends_with='.DS_Store', 
                    id_varname = '', date_prefix='records_'))
        else: 
            setattr(dated_files, folder, 
                DatedDataFiles(files[folder], ignore_files_ends_with='.DS_Store', 
                    id_varname = 'id', date_prefix='records_'))
    
    filename_filter_7d = DatedFilenameFilter(pd_date, days = 7, no_newer=True)
    filename_filter_14d = DatedFilenameFilter(pd_date, days = 14, no_newer=True)
    
    dated_files.wordindex.apply_file_filter(filename_filter_7d)
    if filter_keyword:
        wordindex_filter = DatedWordindexFilter(dated_files.wordindex.files, filter_keyword)
    else:
        wordindex_filter = None

    loader = DataLoader(s3access)
    data_original = loader.load(dated_files.original, filename_filter_7d, wordindex_filter)
    print(data_original.head())
    data_loader_json = DataLoaderJson()
    data_retweet = data_loader_json.load_data_from_single_file(
        s3access, dated_files.retweet.files[0])
    print(data_retweet.head())
    data_words = loader.load(dated_files.words, filename_filter_7d, wordindex_filter)
    print(data_words.head())
    data_sentiments = loader.load(dated_files.sentiments, filename_filter_14d, wordindex_filter)
    print(data_sentiments.head())
    data_emotions = loader.load(dated_files.emotions, filename_filter_14d, wordindex_filter)
    print(data_emotions.head())

    stat_sentiments = calc_stat_sentiments(data_sentiments)
    stat_emotions = calc_stat_emotions(data_emotions)
    tw_rtw_data = TweetRetweetData(data_original, data_retweet, data_words, pd_date)
    stat_words, top_tweets, top_users = (getattr(tw_rtw_data, x) for x in ('stat_words', 'top_tweets', 'top_users'))
    
    print(stat_sentiments.head())
    print(stat_emotions.head())
    print(stat_words.head())
    print(top_tweets.head())
    print(top_users.head())

    stats = {
         'stat_sentiments': stat_sentiments.to_json(orient='split'),
         'stat_emotions': stat_emotions.to_json(orient='split'),
         'stat_words': stat_words.to_json(orient='split'),
         'top_tweets': top_tweets.to_json(orient='split'),
         'top_users': top_users.to_json(orient='split'),
         'time': date + ' ' + '23:59',
         'type': 'filtered stats'
    }
    return json.dumps(stats)


if __name__=='__main__':

    event = {
        'city': 'all_v1', 
        'date':'2020-08-25',
        'filter_keyword':'protest'
    }

    result = lambda_handler(event)
    # print(result['stat_sentiments'])
    # print(result['stat_emotions'])
    # print(result['stat_words'])
    # print(result['top_tweets'])
    # print(result['top_users'])



