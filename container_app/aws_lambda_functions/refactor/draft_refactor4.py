import json
import boto3
from io import BytesIO
import numpy as np
import pandas as pd
import itertools 
from datetime import datetime
from dateutil import tz
from collections import Counter
from copy import deepcopy
from cached_property import cached_property


bucket_name = 'kotasstorage1'
session = boto3.Session()
s3_client = session.client("s3")
s3_resource = boto3.resource('s3')
bucket = s3_resource.Bucket(bucket_name)


# -------------- utilities -------------------
def str_contains_all_strings_in_list(str1, list1):
    return all([str1.find(x)>0 for x in list1])

def str_between(x, str_before, str_after):
    if str_contains_all_strings_in_list(x, [str_before, str_after]):
        return x.split(str_before)[1].split(str_after)[0]
    else:
        raise ValueError(f'"{str_before}" or "{str_after}" is not found in "{x}"')

def extract_timestamp_from_filename(filename, prefix, suffix='.'):
    return pd.Timestamp(str_between(filename, prefix, suffix).replace('_',' '))

def num_to_str2d(x):
    return f'0{x}' if len(str(x))==1 else f'{x}'

def str_date(x):
    return str(x.date())

def idx_timestamps_within_x_days_older(base_timestamp, timestamps, days):
    return [base_timestamp - timestamp <= pd.Timedelta(days, unit='d') for timestamp in timestamps]

def idx_timestamps_within_x_days_newer(base_timestamp, timestamps, days):
    return [timestamp - base_timestamp <= pd.Timedelta(days, unit='d') for timestamp in timestamps]

def keep_files_newer_or_within_x_days_old(
        files, base_timestamp, days = 14, date_prefix = 'created_at_', date_suffix ='.'):
    timestamps = [extract_timestamp_from_filename(file, date_prefix, date_suffix) for file in files ]
    keep_idx = idx_timestamps_within_x_days_older(base_timestamp, timestamps, days)
    return list(itertools.compress(files, keep_idx))

def keep_files_within_x_days_old(
        files, base_timestamp, days = 14, date_prefix = 'created_at_', date_suffix ='.'):
    timestamps = [extract_timestamp_from_filename(file, date_prefix, date_suffix) for file in files ]
    keep_idx_older = idx_timestamps_within_x_days_older(base_timestamp, timestamps, days)
    keep_idx_newer = idx_timestamps_within_x_days_newer(base_timestamp, timestamps, days=0)
    keep_idx = np.multiply(keep_idx_older, keep_idx_newer)
    return list(itertools.compress(files, keep_idx))

def convert_floats(df, target_dtype='float32', include=['float64'], exceptions=None):
    floats = df.select_dtypes(include=include).columns.tolist()
    if exceptions:
        floats  = [ x for x in floats if x not in list(floats)]
    df[floats] = df[floats].astype(target_dtype)
    return None

def path_ends_with_slash(path):
    if path.endswith('/'):
        return path
    else:
        return path + '/'

def filter_df_by_id0(df, id_varname, ids):
    df_ids = pd.DataFrame(ids).set_index(0)
    df_filtered = (df.set_index(id_varname)
         .join(df_ids, how='inner')
         .reset_index().rename(columns={'index':id_varname})
        )
    return df_filtered 

def filter_df_by_id(df, id_varname, ids):
    ids = [id for id in ids if id in list(df[id_varname])]
    if len(ids)==0: return pd.DataFrame(columns=df.columns)
    return df.set_index(id_varname).loc[ids].reset_index()
    

def isBytes(obj):
    return type(obj)==type(b'abc')


def isStr(x): return type(x)==str

def isNum(x): 
    try:
        return type(round(x))==int
    except:
        return False

def tolist(x):
    if isStr(x) or isNum(x): return [x]
    else: return list(x)

def remove_list_items_ending_with(items, str_or_iterable):
    iterlist = tolist(str_or_iterable)
    return [item for item in items if not any([item.endswith(s) for s in iterlist])]
    
def convert_vars_to_numeric(df, vars):
    for var in tolist(vars):
        df[var] = pd.to_numeric(df[var])
    return None

def convert_vars_to_str(df, vars):
    for var in tolist(vars):
        df[var] = df[var].astype(str)
    return None

def convert_index_to_str(df):
    df.index = df.index.astype(str)
    return None

def isCounter(x): 
    return type(x) == type(Counter(('a','b','c')))

def make_var_counter(df, varname):
    item1 = df[varname].get(0, True)
    if type(item1)==list:
        df[varname]= df[varname].apply(lambda x: Counter(dict(x)))  
    elif not isCounter(item1): 
        df[varname]= df[varname].apply(lambda x: Counter(x))  
    return None
 
def is_xth_column_counter(df, x=0):
    return isCounter(df.iloc[:,x].reset_index(drop=True).get(0, False))
     
def remove_null_on_str_var(df, varname, null=''):
    return df[df[varname]!=null]

def remove_null_on_num_var(df, varname, null=''):
    return df[df[varname].notnull()]

def get_top_counts_cases(df, varname, n_top = 15):
    return df[varname].value_counts()[:n_top]

def drop_duplicates_index(df, keep='first'):
    tmp = df.reset_index()
    index_var = tmp.columns[0]
    tmp.drop_duplicates(subset = index_var, keep=keep, inplace=True)
    return tmp.set_index(index_var)

def bring_a_column_first(df, varname):
    cols = [col for col in df.columns if col != varname]
    return df[[varname, *cols]]

def isPandasTimestamp(x):
    return type(x)==type(pd.to_datetime('2020-09-17'))

def make_var_pandas_timestamp(df, varname):
    if not isPandasTimestamp(df[varname].get(0,True)):
        df[varname]= pd.to_datetime(df[varname])
    return None

def extract_hour_from_pandas_timestamp(df, varname):
    make_var_pandas_timestamp(df, varname)
    return df[varname].apply(lambda x: x.hour)
    
def time_now(tzname='America/Denver'):
    return datetime.now(tz.gettz(tzname))

def time_now_pandas(tzname='America/Denver'):
    now = time_now(tzname)
    return pd.to_datetime(now.strftime('%Y-%m-%d %H:%M:%S')) 

class PlaceHolder(): pass



# -------------- s3DataAccess -------------------

class S3DataAccess:
    def __init__(self, bucket_name, s3_client, s3_resource):
        self.bucket_name = bucket_name
        self.s3_client = s3_client
        self.s3_resource = s3_resource
        self.bucket = s3_resource.Bucket(bucket_name)

    def get_data(self, filename):
        f = BytesIO()
        self.s3_client.download_fileobj(self.bucket_name, filename, f)
        return f.getvalue()

    def get_files(self, prefix):
        files = [object.key for object in self.bucket.objects.filter(Prefix=prefix)]
        return sorted(files)


# -------- datedDataFiles ----------------

class DatedFiles:  
    def __init__(self, files=None, ignore_files_ends_with='', date_prefix='records_', date_suffix='.'):
        if files is None:
            self.files = []
        else:
            if ignore_files_ends_with:
                files = remove_list_items_ending_with(files, ignore_files_ends_with)
            self.files = sorted(list(files))
        self.date_prefix = date_prefix
        self.date_suffix = date_suffix

    @property 
    def timestamps(self):
        if self.files:
            return [extract_timestamp_from_filename(file, self.date_prefix, self.date_suffix) for file in self.files ]
        else:
            raise ValueError('found no files')

    @property
    def dates(self):
        if self.files:
            return [str_date(timestamp) for timestamp in self.timestamps]
        else:
            raise ValueError('found no files')


class DatedDataFiles(DatedFiles):
    def __init__(self, files, ignore_files_ends_with='', id_varname='', filekey_unit='date', date_prefix='records_', date_suffix='.'):
        super().__init__(files, ignore_files_ends_with, date_prefix, date_suffix)
        self.id_varname = id_varname
        self.filekey_unit = filekey_unit
        self.file_format = self.files[0].split('.')[1]
        self.filtered_ids = {}

    @property
    def filekeys(self):
        if self.filekey_unit=='date': 
            return sorted(self.dates)
        else:
            raise NotImplementedError
    
    def apply_file_filter(self, file_filter):
        self.files = file_filter.filter_files(self)

    def apply_id_filter(self, index_filter, DataLoader=None, DataAccess=None):
        if not self.id_varname: raise ValueError('please set "id_varname".')
        self.filtered_ids = index_filter.filter_ids(self, DataLoader, DataAccess)


class DatedFilenameFilter:
    def __init__(self, base_timestamp, days = 7, no_newer=True):
        self.base_timestamp = base_timestamp
        self.days = days
        self.no_newer = no_newer

    def filter_files(self, DatedDataFiles):
        if self.no_newer:
            func = keep_files_within_x_days_old
        else: 
            func = keep_files_newer_or_within_x_days_old
        return func(
                files=DatedDataFiles.files, base_timestamp=self.base_timestamp, days=self.days, 
                date_prefix = DatedDataFiles.date_prefix, date_suffix = DatedDataFiles.date_suffix)



class DatedWordindexFilter:
    def __init__(self, files, filter_keyword, ignore_files_ends_with=''):
        if ignore_files_ends_with:
            files = remove_list_items_ending_with(files, ignore_files_ends_with)
        self.files = list(files)
        self.file_format = self.files[0].split('.')[1]
        self.filter_keyword = filter_keyword

    def get_filtered_ids(self, DatedDataFiles, DataLoader, DataAccess):
        dict_id = {}
        for file in self.files:
            datetime = str_between(file, DatedDataFiles.date_prefix, DatedDataFiles.date_suffix)
            try:
                with open(file) as f:
                    data = json.load(f)
            except Exception:
                data = DataLoader.load_data_from_single_file(DataAccess, file)
            except ValueError:
                print('DataAccess is missing get_data() method')
            dict_id[datetime] = data[self.filter_keyword]
        return dict_id

    def filter_ids(self, DatedDataFiles, DataLoader=None, DataAccess=None):
        filtered_ids = self.get_filtered_ids(DatedDataFiles, DataLoader, DataAccess)
        return { key: value for key, value in filtered_ids.items() if key in DatedDataFiles.filekeys }




# -------- dataLoaderMethods -------------


class DataLoaderBase:
    def load_data(self, DataFiles, DataAccess):
        df = []
        for file, filekey in zip(DataFiles.files, DataFiles.filekeys):
            filtered_ids = DataFiles.filtered_ids.get(filekey, [])
            data = self.read_data(self.get_data(DataAccess, file))
            if len(filtered_ids):
                data = filter_df_by_id(data, DataFiles.id_varname, filtered_ids)   
            if len(data):
                df.append(data)  
        self.df = pd.concat(df, ignore_index=True) 

    def load_data_from_single_file(self, DataAccess, file):
        return self.read_data(self.get_data(DataAccess, file))

    def convert_floats(self, *args, **kwargs):
        self.df = convert_floats(self.df, args, kwargs)  

    def get_data(self): pass

    def read_data(self): pass


class DataLoaderJson(DataLoaderBase):
    def __init__(self, orient='records', lines=True, float_dtype='float32'):
        self.orient = orient
        self.lines = lines
        self.float_dtype = float_dtype

    def get_data(self, DataAccess, file):
        return DataAccess.get_data(file)

    def read_data(self, data):
        return pd.read_json(data, orient=self.orient, lines=self.lines)

    def load_data(self, DataFiles, DataAccess):
        super().load_data(DataFiles, DataAccess)
        convert_floats(self.df, target_dtype=self.float_dtype, 
                       exceptions=DataFiles.id_varname)


class DataLoaderCSV(DataLoaderBase):
    def __init__(self, encoding='latin-1', float_dtype='float32'):
        self.float_dtype = float_dtype
        self.encoding = encoding

    def get_data(self, DataAccess, file):
        data = DataAccess.get_data(file)
        if isBytes(data):
            return BytesIO(data)
        else:
            return data

    def read_data(self, data):
        return pd.read_csv(data, encoding=self.encoding)

    def load_data(self, DataFiles, DataAccess):
        super().load_data(DataFiles, DataAccess)
        convert_floats(self.df, target_dtype=self.float_dtype, 
                       exceptions=DataFiles.id_varname)
        

# ------- factory and DataLoader ----------
class Factory():
    def __init__(self):
        self._tools = {}

    def register_tool(self, key, tool):
        self._tools[key] = tool

    def get_tool(self, key):
        tool = self._tools.get(key)
        if not tool:
            raise ValueError(key)
        return tool() 

data_loader_factory = Factory()
data_loader_factory.register_tool('json', DataLoaderJson)
data_loader_factory.register_tool('csv', DataLoaderCSV)

class DataLoader:

    def __init__(self, DataAccess):
        self.DataAccess = DataAccess

    def load(self, DataFiles, FileFilter=None, IndexFilter=None):
        data_loader = data_loader_factory.get_tool(DataFiles.file_format)
        if FileFilter: DataFiles.apply_file_filter(FileFilter)
        if IndexFilter: 
            indexdata_loader = data_loader_factory.get_tool(IndexFilter.file_format)
            DataFiles.apply_id_filter(IndexFilter, indexdata_loader, self.DataAccess)
        data_loader.load_data(DataFiles, self.DataAccess)
        return data_loader.df


# -------- TweetRetweetData -------

def get_subset_times(now=None):
    if now is None:
        now = time_now_pandas(tzname='America/Denver')
    
    now_1h = now + pd.DateOffset(hours=-1)
    today = now.floor("d")
    yesterday = today + pd.DateOffset(days=-1)
    seven_d_ago = today + pd.DateOffset(days=-7)    
    return now, now_1h, today, yesterday, seven_d_ago

def create_time_subsets_of_df(df, timevar='created_at_h', now=None):
    now, now_1h, today, yesterday, seven_d_ago = get_subset_times(now)
    
    df = deepcopy(df)
    make_var_pandas_timestamp(df, timevar)
    if hasattr(df, '_timevar_d') or hasattr(df, '_hour'):
        raise AttributeError('Variable name already exists: avoid names "_timevar_d" and "_hour".')
    df['_timevar_d'] = df[timevar].dt.floor('d')
    df['_hour'] = extract_hour_from_pandas_timestamp(df, timevar)

    subsets = PlaceHolder()
    idx_today = df._timevar_d == today
    for h in range(24):
        setattr(subsets,'hour_' + str(h), df[(df._hour==h) & (idx_today)]) 

    subsets.today = df[df._timevar_d==today]
    subsets.yesterday = df[df._timevar_d==yesterday]
    subsets.seven_days = df[(df._timevar_d <= today) & (df._timevar_d >=seven_d_ago)]
    return(subsets)

    
class TweetRetweetData:
    def __init__(self, cum_ori, cum_rt, cum_words, now=None):
        self.cum_ori = create_time_subsets_of_df(cum_ori, timevar='created_at_h', now=now)
        self.cum_rt = cum_rt
        self.cum_words = create_time_subsets_of_df(cum_words, timevar='created_at_h', now=now)
        self.now = now
        self.subset_names = ['today', 'yesterday', 'seven_days'] + ['hour_' + str(h) for h in range(24)]

    @cached_property
    def stat_words(self):
        subs = self.cum_words
        stat_words = []
        for subset in self.subset_names:
            stat_words.append(calc_stat_words(
                df_words_sample_and_batch_sum(getattr(subs, subset)),
                subset_name=subset))
        return pd.concat(stat_words, ignore_index=True)

    @cached_property
    def top_tweets(self):
        subs = self.cum_ori
        cum_rt = self.cum_rt
        top_tweets = []
        for subset in self.subset_names:
            top_tweets.append(calc_top_tweets(getattr(subs, subset), cum_rt, subset_name=subset))
        return pd.concat(top_tweets, ignore_index=True)

    @cached_property
    def top_users(self):
        subs = self.cum_ori
        cum_rt = self.cum_rt
        top_users = []
        for subset in self.subset_names:
            top_users.append(calc_top_users(getattr(subs, subset), cum_rt, subset_name=subset))
        return pd.concat(top_users, ignore_index=True)


# -------- TweetRetweetStats -----

def calc_stat_sentiments(df_sentiments):
    stat = (df_sentiments[['created_at_h', 'compound']]
     .groupby('created_at_h')
     .agg(['mean','count'])
     .round(3)
     .sort_values("created_at_h") 
     .reset_index()
    )

    stat.columns =  stat.columns.droplevel()
    stat = stat.rename(columns ={'': 'time'})
    return stat


def calc_stat_emotions(df_emotions):
    emo_labels = [*df_emotions.columns][2:]
    convert_vars_to_numeric(df_emotions, emo_labels)

    stat = (df_emotions[['created_at_h', *emo_labels]]
     .groupby('created_at_h')
     .agg('mean')
     .round(3)
     .sort_values("created_at_h") 
     .reset_index()
    )

    stat = stat.rename(columns ={'created_at_h':'time'})
    return stat


def calc_stat_words(df_words, subset_name=''):
    words = df_words.dropna(subset=['token_counter'])
    if len(words)==0: 
        stat_words = pd.DataFrame(data={'token_counter':[{}],'count':[0]})
    else:
        stat_words = words[['token_counter', 'count']].agg(['sum'])
    if subset_name:
        stat_words = add_subset_name(stat_words, subset_name) 
    return stat_words


def calc_top_tweets(ori_data, cum_rt_data, subset_name='', num_top_tweets=15): 
    top_tweets_calc = TopTweetsCalculator(ori_data, cum_rt_data, num_top_tweets)
    top_tweets = top_tweets_calc.top_tweets
    if subset_name:
        top_tweets = add_subset_name(top_tweets, subset_name)
    return top_tweets
    
    

def calc_top_users(ori_data, cum_rt_data, subset_name='', num_top_users=15):
    top_users_calc = TopUsersCalculator(ori_data, cum_rt_data, num_top_users)
    top_users = top_users_calc.top_users
    if subset_name:
        top_users = add_subset_name(top_users, subset_name)
    return top_users


def add_subset_name(df, subset_name):
    df['subset_name'] = subset_name
    return bring_a_column_first(df, 'subset_name')


class TopTweetsCalculator:
    def __init__(self, ori_data, cum_rt_data, num_top_tweets=15):
        convert_vars_to_str(ori_data, 'RT_id')
        convert_vars_to_str(cum_rt_data, 'RT_id')
        self.ori_data = remove_null_on_str_var(ori_data, 'RT_id')
        self.cum_rt_data = cum_rt_data
        self.num_top_tweets = min(num_top_tweets, len(ori_data))

    @property
    def df_null(self):
        df_null = pd.DataFrame(
                    columns=['RT_id','user_name', 'followers_count', 'text', 't_co',
                       'tags', 'retweet_timespan', 'retweet_total'])
        return df_null
    
    @cached_property
    def top_RT_ids(self):
        top_RT_ids = get_top_counts_cases(self.ori_data, 'RT_id', self.num_top_tweets)
        return top_RT_ids
    
    @cached_property
    def top_RT_data(self):
        top_RT_ids_no_missing = [id for id in self.top_RT_ids.index if id in list(self.cum_rt_data.RT_id)]
        top_RT_data = (self.cum_rt_data[['RT_id','user_name','followers_count','text','t_co','tags']]
                       .set_index('RT_id')
                       .loc[top_RT_ids_no_missing]
                       )
        if len(top_RT_data)==0: return None
        top_RT_data = drop_duplicates_index(top_RT_data, keep='last') 
        return top_RT_data
    
    @cached_property
    def top_RT_stats(self):
        top_RT_stats = (self.ori_data[['RT_id','RT_retweet_count']]
                        .set_index('RT_id')
                        .loc[self.top_RT_ids.index]
                        .reset_index()
                        .groupby('RT_id')
                        .agg(['count', 'max'])
                        .reset_index()
                       )
        top_RT_stats.columns = top_RT_stats.columns.droplevel(level=0)
        top_RT_stats = (top_RT_stats
                        .rename(columns={'':'RT_id', 
                                         'count':'retweet_timespan',
                                         'max':'retweet_total'})
                        .set_index('RT_id')
                       )
        return top_RT_stats
    
    @cached_property
    def top_tweets(self):
        if len(self.ori_data)==0 or self.top_RT_data is None: return self.df_null
        top_tweets = (
            self.top_RT_data
            .join(self.top_RT_stats)
            .sort_values(by=['retweet_timespan'], ascending=False)
            .reset_index()
            )
        return top_tweets
    


class TopUsersCalculator:
    def __init__(self, ori_data, cum_rt_data, num_top_users=15):
        convert_vars_to_str(ori_data, 'RT_id')
        convert_vars_to_str(cum_rt_data, ['RT_id','user_id'])
        self.ori_data = remove_null_on_str_var(ori_data, 'RT_id')
        self.cum_rt_data = cum_rt_data
        self.num_top_users = min(num_top_users, len(ori_data))

    @property
    def df_null(self):
        df_null = pd.DataFrame(
            columns = ['user_id', 'RT_id', 'user_name', 'user_description',
                       'followers_count', 'following_count', 'retweeted'])
        return df_null

    @cached_property
    def top_RT_ids(self):
        RT_id_to_user_id_coverage_factor = 3
        top_RT_ids = get_top_counts_cases(self.ori_data, 'RT_id', 
                                          self.num_top_users * RT_id_to_user_id_coverage_factor)
        return top_RT_ids
    
    @cached_property
    def userdata_of_top_RT(self):
        userdata_of_top_RT = (
            self.ori_data[['RT_id']].set_index('RT_id').loc[self.top_RT_ids.index]  
            .join(self.cum_rt_data[['RT_id','user_id','user_name','user_description',
                                    'followers_count','following_count']].set_index('RT_id'),
                  how='inner')
            .reset_index()
            .dropna(subset=['user_id'])
            )
        return userdata_of_top_RT
    
    @cached_property
    def top_users_ids(self):
        top_user_ids = get_top_counts_cases(self.userdata_of_top_RT, 'user_id', self.num_top_users) 
        return top_user_ids

    @cached_property
    def top_users_data(self):
        top_users_data = (
            self.userdata_of_top_RT.set_index('user_id').loc[self.top_users_ids.index]
            .reset_index()
            .drop_duplicates(subset = 'user_id', keep='last')
        )
        if len(top_users_data)==0: return None
        return top_users_data
    
    @cached_property
    def top_users(self):
        if len(self.ori_data)==0 or self.top_users_data is None: return self.df_null
        top_users = (self.top_users_data.set_index('user_id')
          .join(self.top_users_ids)
          .rename(columns = {'user_id':'retweeted'})
          .reset_index()
          .sort_values(by = ['retweeted','followers_count'], ascending = False)
         )
        return top_users
    

# -------- BatchDfProcessor ----------   
    
class BatchDfProcessor:
    def __init__(self, df, batch_size):
        self.df = df
        self.batch_size = batch_size
        self.batches = np.floor((len(df)/batch_size))
        self.last_batch_size = len(df) % batch_size
        self.processed_df = pd.DataFrame()
    
    def process(self, func, *args, **kwargs):
        b = 0
        i_beg = 0
        i_end = self.batch_size
        processed = []
        while b < self.batches:
            batch_result = func(self.df[i_beg:i_end], *args, **kwargs)
            processed.append(batch_result)
            b += 1
            i_beg += self.batch_size
            i_end = self.find_next_batch_size(b, i_end)
            if i_end is None: break
        self.processed_df = pd.concat(processed, ignore_index=True)
            
    def find_next_batch_size(self, b, i_end):
        if b == self.batches: 
            if self.last_batch_size==0:
                return None
            else:
                i_end += self.last_batch_size
        else:
            i_end += self.batch_size
        return i_end


def df_words_sample_and_batch_sum(df_words, batch_size = 100, num_to_sample = 10000):
    df_null = pd.DataFrame(columns=['token_counter','count'])
    if len(df_words)==0: return df_null
    words = deepcopy(df_words)
    make_var_counter(words, 'token_counter')
    words = words[['token_counter']].dropna()
    if len(words)==0: return df_null

    if len(words) < num_to_sample:
        df = words
    else:
        df = words.sample(n=num_to_sample)

    batch_df_processor = BatchDfProcessor(df, batch_size)
    batch_df_processor.process(sum_df_counter_most_common, 
                               newvarname='token_counter', 
                               num_most_common=200)
        
    make_var_counter(batch_df_processor.processed_df, 'token_counter')
    return batch_df_processor.processed_df
     

def sum_df_counter_most_common(df, newvarname='token_counter', num_most_common=200):
    result = sum_df_counter(df, newvarname)
    result[newvarname] = result[newvarname].apply(lambda x: x.most_common(num_most_common))
    return result   

def sum_df_counter(df, newvarname='token_counter'):
    if len(df.columns) > 1 or not is_xth_column_counter(df,0): 
        raise TypeError(f'"df" must contain only a Counter variable.:df = {df}')
        
    a = df.agg(['sum', 'count'])
    a = a.transpose().rename(columns = {'sum':newvarname})
    return a


# -------- main ---------------
def lambda_handler(event, context):
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

