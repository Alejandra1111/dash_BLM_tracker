import numpy as np
import pandas as pd
from datetime import datetime
import json
import os
from glob import glob 
import itertools

from summarizing_helpers import *



data_path = '/Users/kotaminegishi/big_data_training/python/dash_demo1/'
data_dest = '/Users/kotaminegishi/big_data_training/python/dash_demo1/'


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
                     glob(data_path + 'data_cumulative/retweet/*')[0]))

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
	                        file_type = '.csv', days=14) 
	    if len(files_sentiments)>0: 
	        cum_sentiments = tw_data_files_to_df_csv(files_sentiments)
	        cum_sentiments = cum_sentiments.drop_duplicates(subset = 'id')
	        fix_datetime(cum_sentiments)
	        stat_sentiments = calc_stat_sentiments(cum_sentiments)
	    else:
	        stat_sentiments = null_stat_sentiments
	        
	    files_emotions = keep_recent_files(glob(cum_data_path + "/emotions/*"),
	                        base_timestamp=base_timestamp, prefix='records_',
	                        file_type = '.csv', days=14)
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
	                                file_type = '.json', days=7) 

	if len(files_words)>0:
	    cum_words = tw_data_files_to_df_json(files_words, lines=True)
	    fix_datetime(cum_words)
	    fix_token_counter(cum_words)
	else:
	    cum_words = null_cum_words

	print('  Loading cumulative data: original tweets and retweets...')   
	# load recent cumulative data     
	files_original = keep_recent_files(glob(cum_data_path + "/original/*"),
	    base_timestamp = base_timestamp, days=7,
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

		# curr_data_path = data_path + "data_current/city/" + city

		# # update current data: recent cumulative files
		# stat_sentiments.to_csv(curr_data_path + '/stat_sentiments.csv', index = False)
		# stat_emotions.to_csv(curr_data_path +  '/stat_emotions.csv', index = False)
		# print('  Updated current data: stat_sentiments and stat_emotions.')

		# stat_words.to_json(curr_data_path + '/stat_words.json', orient='records', lines=True)
		# top_users.to_csv(curr_data_path + '/top_users.csv', index=False)
		# top_tweets.to_csv(curr_data_path + '/top_tweets.csv', index=False)
		# print('  Updated current city data: stat_words, top_users, and top_tweets.')

		print('Updating city stats:')
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

			hour_words = hour_words.append(
				stat_words0[stat_words0.subset.apply(lambda x: x.startswith('hour_'))])
			hour_tweets = hour_tweets.append(
				top_tweets0[top_tweets0.subset.apply(lambda x: x.startswith('hour_'))])
			hour_users = hour_users.append(
		top_users0[top_users0.subset.apply(lambda x: x.startswith('hour_'))])

		if len(repair_stats_datetime)>0:
			for datetime in repair_stats_datetime:
				print('repairing hourly data for:', datetime)
				tmp_sent, stat_emo, tmp_words, tmp_tweets, tmp_users = get_city_data(city, data_path, pd.to_datetime(datetime), hour_stats_only=True)

				print(tmp_words)
				hour_words = hour_words.append(tmp_words)
				print(hour_words)
				hour_tweets = hour_tweets.append(tmp_tweets)
				hour_users = hour_users.append(tmp_users)

		if len(hour_words)>0:

			stat_words = consolidate_stats(stat_words, hour_words)
			top_tweets = consolidate_stats(top_tweets, hour_tweets)
			top_users = consolidate_stats(top_users, hour_users)
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



   
def fix_datetime(df, timevar='created_at_h'):
    df[timevar] = pd.to_datetime(df[timevar])

def fix_token_counter(df):
    df.token_counter = df.token_counter.apply(lambda x: Counter(x))  

def fix_RT_id(df):
    df.RT_id = df.RT_id.astype(str) 


def convert_floats(df, float_dtype='float32'):
    floats = df.select_dtypes(include=['float64']).columns.tolist()
    df[floats] = df[floats].astype(float_dtype)
    return df

def tw_data_files_to_df_csv(files):
    '''append and concat data files into a pandas.DataFrame'''
    df = []
    [ df.append(pd.read_csv(file)) for file in files ]
    df = pd.concat(df, ignore_index=True)
    return df


def tw_data_files_to_df_json(files, lines=False):
    '''append and concat data files into a pandas.DataFrame'''
    df = []
    [ df.append(pd.read_json(file, orient='records', lines=lines)) for file in files ]
    df = pd.concat(df, ignore_index=True)
    return df



def keep_recent_files(files, base_timestamp, file_type= '.json', days = 14, no_newer=False,
                      prefix = 'created_at_'):
    timestamps = [pd.Timestamp(file.split(prefix,1)[1]
                               .replace(file_type,'').replace('_',' ')) for file in files ]
    if no_newer: 
        keep_idx1 = [(base_timestamp - timestamp <= pd.Timedelta(days, unit='d')) & 
                     (base_timestamp - timestamp >= pd.Timedelta(0, unit='d')) for timestamp in timestamps]
    else: 
        keep_idx1 = [base_timestamp - timestamp <= pd.Timedelta(days, unit='d') for timestamp in timestamps]
    return(list(itertools.compress(files,keep_idx1)))


def files_read_update(files, filename, mode='a', 
        colname = 'name', header=False, index=False):
        pd.DataFrame(files, 
                 columns = {colname}
                ).to_csv(filename + '.csv', 
                         mode=mode, header=header, index=index)



if __name__=="__main__":


	cities = ['Minneapolis','LosAngeles','Denver','Miami','Memphis',
	          'NewYork','Louisville','Columbus','Atlanta','Washington',
	          'Chicago','Boston','Oakland','StLouis','Portland',
	          'Seattle','Houston','SanFrancisco','Philadelphia','Baltimore']

	#cities = []
	cities_all = ['all_v1','all_v2','all_v3','all_v4','all_v5']
	cities_all = []
	
	# data_path = '/Users/kotaminegishi/big_data_training/python/dash_demo1/'
	# data_dest = '/Users/kotaminegishi/big_data_training/python/dash_demo1/'
	data_dest_files = data_dest + 'data_cumulative/'

	list_days = range(8,32)

	#days_to_keep = 1 # batch process 3 days
	#days_to_process= 3
	#days_to_process= 1 #  matching original tweet ids, may allow for a few days of time lag



	# for city in cities + cities_all:

	# 	# Parent Directory path 
	# 	parent_dir = data_path + "data_cumulative/city_date/"

	# 	# Path 
	# 	path = parent_dir + city + '/stats'

	# 	dir_exist = os.path.isdir(path)
	# 	if not dir_exist:
	# 	    # Create the directory 
	# 	    os.mkdir(path) 
	# 	    print("city_date/stats directory for '%s' created" %city) 



	for d in list_days:
		process_timestamp = pd.to_datetime(datetime(2020,7, d, 23))
		print('process_timestamp: ', process_timestamp)   
		current_datetime = process_timestamp.floor('h')
		
		# include potential repair candidates for the past hrs of the same date 
		all_stats_datetime = [str(current_datetime + pd.Timedelta(-h, unit='h')) for h in range(1, current_datetime.hour+1)]
		#all_stats_datetime = [str(current_datetime + pd.Timedelta(-h, unit='h')) for h in range(1,24)]

		try:
		    # read previous filenames
		    existing_stats_datetime = pd.read_csv(data_dest + 'data_filenames/files_stats.csv')
		    # get new file names 
		    repair_stats_datetime = [datetime for datetime in all_stats_datetime if datetime not in list(existing_stats_datetime.datetime)]
		    if len(repair_stats_datetime)>0: print('Repairing city_date stats for the following datetime: ', repair_stats_datetime)
		    mode = 'a'
		    header = False

		except:
		    # initialize filenames 
		    repair_stats_datetime = all_stats_datetime
		    mode = 'w'
		    header = True


		stats_datetime = repair_stats_datetime.copy()
		stats_datetime.append(str(current_datetime))
		print(repair_stats_datetime)
		update_current_data_city(cities_all + cities, data_dest, process_timestamp, repair_stats_datetime)

		files_read_update(stats_datetime, 
		    data_dest + 'data_filenames/files_stats', colname = 'datetime', mode=mode, header =header)
		print('city_date stats updated for: ',  stats_datetime)




