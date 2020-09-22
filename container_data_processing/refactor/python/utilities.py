import numpy as np
import pandas as pd
import itertools
from datetime import datetime
from dateutil import tz
import pytz
from collections import Counter


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

def data_files_to_df_json(files, orient='records', lines=False):
    df = []
    [ df.append(pd.read_json(file, orient=orient, lines=lines)) for file in files ]
    df = pd.concat(df, ignore_index=True)
    return df

def is_dst(dt=None, timezone="UTC"):
    if dt is None:
        dt = datetime.utcnow()
    timezone = pytz.timezone(timezone)
    timezone_aware_date = timezone.localize(dt, is_dst=None)
    return timezone_aware_date.tzinfo._dst.seconds != 0

def is_dst_pd_mtn(pd_dt, timezone='US/Mountain'):
    return is_dst(pd_dt.to_pydatetime(), timezone=timezone)

def convert_time(df, dst_pd_dt, timezone='US/Mountain', varname='created_at'):
    dst = is_dst_pd_mtn(dst_pd_dt, timezone=timezone)
    df[varname] = pd.to_datetime(df.created_at, unit='s') + pd.DateOffset(hours=-7 + dst*1) 

def add_time_floored_at_hour(df, varname='created_at', floored_varname='created_at_h'):
    df[floored_varname] =  df[varname].dt.floor("h")

def keep_if_var_has_positive_length(df, varname):
    keep_index = df[varname].apply(lambda x: len(x)>0)
    return df[keep_index]

def append_and_drop_duplicates(df1, df2, subset):
    return df1.append(df2).drop_duplicates(subset = subset)

def append_a_content_to_file(content, file):
    with open(file,'a') as f:
        f.write(content)

def append_a_list_to_csv_file(list, varname, filename):
    (pd.DataFrame(list, columns = {varname})
     .to_csv(filename, mode='a', header=False, index=False)
    )
    


