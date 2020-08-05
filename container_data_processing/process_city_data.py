import numpy as np
import pandas as pd
from datetime import datetime
import json
import os
from glob import glob 
import itertools


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

def tw_data_files_to_df_csv2(files, frac=0.05, float_dtype=None):
    '''append and concat a sample of data into a pandas.DataFrame'''
    df = []
    [ df.append(pd.read_csv(file, low_memory=True)
        .sample(frac=frac, replace=True)) for file in files ]
    df = pd.concat(df, ignore_index=True)
    if float_dtype is None: return df
    return convert_floats(df, float_dtype)


def tw_data_files_to_df_json(files, lines=False):
    '''append and concat data files into a pandas.DataFrame'''
    df = []
    [ df.append(pd.read_json(file, orient='records', lines=lines)) for file in files ]
    df = pd.concat(df, ignore_index=True)
    return df


def tw_data_files_to_df_json3(files, lines=False, frac=0.05, float_dtype=None, verbose=False):
    '''append and concat a sample of data into a pandas.DataFrame'''
    df = []
    for file in files:
        if verbose: print('loading ' + file)
        df.append(pd.read_json(file, orient='records', lines=lines)
                 .sample(frac=frac, replace=True)) 
    df = pd.concat(df, ignore_index=True)
    if float_dtype is None: return df
    return convert_floats(df, float_dtype)


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
    
    if city_type not in ['city','all']: raise ValueError('city_type must be "city" or "all".')
    if len(files)==0: return None
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
                mode = 'a' if new_file else 'w'
                df_date.to_csv(data_path + filename, index=False, mode=mode)
            if new_file: print('created: ', filename)
            else: print('appended: ', filename)
            


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

	    
	city_filterwords = {
	    'Minneapolis': ['Minneapolis','mlps', ['St', 'Paul']],
	    'LosAngeles':['LosAngeles','LA', ['Los', 'Angeles']],
	    'Denver': ['Denver', 'DEN'],
	    'Miami': ['Miami'],
	    'Memphis': ['Memphis'],
	    'NewYork': ['NewYork',['New','York'], 'NY','NYC','manhattahn'],
	    'Louisville': ['Louisville'],
	    'Columbus': ['Columbus'],
	    'Atlanta': ['Atlanta'],
	    'Washington': ['Washington','DC','WashingtonDC'],
	    'Chicago': ['Chicago'],
	    'Boston': ['Boston'],
	    'Oakland': ['Oakland'],
	    'StLouis': ['StLouis',['St','Loius']],
	    'Portland': ['Portland'],
	    'Seattle': ['Seattle'],
	    'Houston': ['Houston'],
	    'SanFrancisco': ['SanFrancisco','SF',['San','Francisco']],
	    'Philadelphia': ['Philadelphia'],
	    'Baltimore': ['Baltimore']
	}

	cities_all = ['all_v1','all_v2','all_v3','all_v4','all_v5']
	
	#data_dest = '/Users/kotaminegishi/big_data_training/python/dash_demo1/'
	data_dest = '/data/app_data/'
	data_path = data_dest

	data_dest_files = data_dest + 'data_cumulative/'
	days_to_keep = 1
	days_to_process = 2


	current_time = datetime.utcnow() + pd.DateOffset(hours=-6)
	current_time_s = current_time.strftime('%Y-%m-%d %H:%M:%S')

	current_time_s = pd.to_datetime(current_time_s)
	base_timestamp = current_time_s
	#base_timestamp =  pd.to_datetime(datetime(2020,7,20))

	print('base_timestamp: ', base_timestamp)	


	for city in cities:

		# Parent Directory path 
		parent_dir = data_path + "data_cumulative/city_date/"

		# Path 
		path = os.path.join(parent_dir, city) 

		dir_exist = os.path.isdir(path)

		if not dir_exist:
			# Create the directory 
			os.mkdir(path) 
			os.mkdir(path + '/original') 
			os.mkdir(path + '/retweet') 
			os.mkdir(path + '/sentiments') 
			os.mkdir(path + '/emotions')
			os.mkdir(path + '/words') 
			print("City_date directory for '%s' created" %city) 


	files_retweet = [data_path + 'data_cumulative/retweet/2020_all_retweets.json']


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

	try:
		# read previous filenames
		files_existing_original = pd.read_csv(data_dest + 'data_filenames/files_read_original.csv')
		files_existing_sentiments = pd.read_csv(data_dest + 'data_filenames/files_read_sentiments.csv')
		files_existing_emotions = pd.read_csv(data_dest + 'data_filenames/files_read_emotions.csv')
		files_existing_words = pd.read_csv(data_dest + 'data_filenames/files_read_words.csv')

		# get new file names 
		files_original = [file for file in files_all_original if file.split(data_dest_files)[1] not in np.array(files_existing_original.name)]
		files_sentiments = [file for file in files_all_sentiments if file.split(data_dest_files)[1] not in np.array(files_existing_sentiments.name)]
		files_emotions = [file for file in files_all_emotions if file.split(data_dest_files)[1] not in np.array(files_existing_emotions.name)]
		files_words = [file for file in files_all_words if file.split(data_dest_files)[1] not in np.array(files_existing_words.name)]

		mode = 'a'
		header = False

	except:
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

	
	#print(files_retweet)
	retweet_files_by_city_json(files_retweet, cities, city_filterwords, data_path, verbose=True)

	df_retweet = pd.read_json(files_retweet[0], orient='records',lines=True)
	for c in cities_all:
		filename = data_path + 'data_cumulative/city_date/' + c + '/retweet/2020_all_retweets.json'
		df_retweet.to_json(filename, orient='records',lines=True)
		print('updated ',  filename)

	#print(files_original)
	original_files_by_city_date_json(files_original, cities, city_filterwords, data_path, verbose=True)
	
	original_files_by_city_date_json(files_original, cities_all, [], 
                                data_path, verbose=True, city_type='all', sample_frac=.1)


	#print('new files_sentiments:', files_sentiments)
	files_id_matched_by_city_date_json(
	    files_sentiments, cities + cities_all, data_path, 'sentiments', 
	    base_timestamp, process_days = days_to_process,
	    file_type='.csv', verbose=True)


	#print('new files_emotions:', files_emotions)
	files_id_matched_by_city_date_json(
	    files_emotions, cities + cities_all, data_path, 'emotions', 
	    base_timestamp, process_days = days_to_process,
	    file_type='.csv', verbose=True)


	#print('new files_words:', files_words)
	files_id_matched_by_city_date_json(
	    files_words, cities + cities_all, data_path, 'words', 
	    base_timestamp, process_days = days_to_process,
	    file_type='.json', verbose=True)
	

	
	# update filenames for those that are read and processed
	files_original_s = [file.split(data_dest_files)[1] for file in files_original]
	files_sentiments_s = [file.split(data_dest_files)[1] for file in files_sentiments]
	files_emotions_s = [file.split(data_dest_files)[1] for file in files_emotions]
	files_words_s = [file.split(data_dest_files)[1] for file in files_words]


	files_read_update(files_original_s, 
		data_dest + 'data_filenames/files_read_original', mode=mode, header =header)

	files_read_update(files_sentiments_s, 
		data_dest + 'data_filenames/files_read_sentiments', mode=mode, header =header)

	files_read_update(files_emotions_s, 
		data_dest + 'data_filenames/files_read_emotions', mode=mode, header =header)

	files_read_update(files_words_s, 
		data_dest + 'data_filenames/files_read_words', mode=mode, header =header)

	

