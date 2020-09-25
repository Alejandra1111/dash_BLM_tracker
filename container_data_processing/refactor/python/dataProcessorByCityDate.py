from glob import glob
from cached_property import cached_property 
import pandas as pd

from utilities import df_saver_json, df_saver_csv, get_unique_dates_in_var, filter_df_by_date, \
    get_a_var_from_jsonfile, keep_files_within_x_days_old, convert_floats, mark_var_in_valuelist, \
    remove_punct_from_tokens, apply_fun_to_dict_data, df_to_json, keep_if_var_in_valuelist

from globals import to_json_args, read_json_args


class DataProcessorByCityDate:
    def __init__(self, files, cities, city_filterwords, data_path='',
                 data_type = 'original', file_type='json', sample_frac = .02,
                 process_datetime = None, process_days = None,
                 rt_filename='2020_all_retweets.json',
                 float_dtype='float16', verbose=True):
        if file_type not in ['json','csv']: raise ValueError('file_type must be "json" or "csv".')
        self.files = list(files)
        self.cities = list(cities)
        self.city_filterwords = city_filterwords
        self.data_path = data_path
        self.rt_filename = rt_filename
        self.float_dtype = float_dtype
        self.verbose = verbose
        self.data_type = data_type
        self.file_type = file_type
        self.sample_frac = sample_frac     
        self.process_datetime = process_datetime
        self.process_days = process_days
        self.df_saver = df_saver_json if file_type=='json' else df_saver_csv
        self.df_reader = pd.read_json if  file_type=='json' else pd.read_csv
    
    def register_city_df_filter(self, city_df_filter):
        self.city_df_filter = city_df_filter

    def process_data(self):
        if len(self.files)==0: return None
        if not hasattr(self, 'city_df_filter'): raise ValueError('Please register city_df_filter.')
        
        print('Found ' + str(len(self.files)) + ' files to process')

        for city in self.cities:
            if len(self.city_df.get(city,[]))==0: continue
            city_data = pd.concat(self.city_df.get(city), ignore_index=True)
            
            if self.data_type == 'retweet':
                filename = f'{self.data_path}data_cumulative/city_date/{city}/retweet/{self.rt_filename}'
                self.df_saver(city_data, filename, **self.df_saver_args)
                print('updated: ', filename.split(self.data_path)[1])

            else:
                dates, dates_str = get_unique_dates_in_var(city_data, 'created_at_h')
                for date in dates_str:
                    if self.verbose: print('processing date of ' + date)  
                    df_date = filter_df_by_date(city_data, 'created_at_h', date)
                    filename = f'{self.data_path}data_cumulative/city_date/{city}/{self.data_type}/records_{date}.{self.file_type}'
                    new_file = glob(filename)==[]
                    self.df_saver(df_date, filename, new_file, **self.df_saver_args)
                    print(new_file *'created: ' + (not new_file) * 'appended:', filename.split(self.data_path)[1])

    @property
    def df_saver_args(self):
        if self.file_type=='csv': return dict()
        str_vars = ['created_at', 'created_at_h']
        str_vars_dict = dict(
            original=str_vars, retweet = str_vars, 
            words = str_vars[1], sentiments = str_vars[1], emotions = str_vars[1]
            )
        args = dict(
            vars_to_str=str_vars_dict.get(self.data_type,[]), 
            read_kwargs=read_json_args, 
            save_kwargs=to_json_args
            )
        return args

    @property
    def df_reader_args(self):
        if self.file_type=='csv': return dict()
        return read_json_args

    @cached_property 
    def city_RT_ids(self):
        city_RT_ids = {}    
        for city in self.cities:
            file = f'{self.data_path}data_cumulative/city_date/{city}/retweet/{self.rt_filename}'
            city_RT_ids[city] = list(get_a_var_from_jsonfile('RT_id', file))
        return city_RT_ids
    

    @cached_property
    def city_ids(self):
        if not self.process_datetime or not self.process_days: 
            raise ValueError('"process_datetime" or "process_days" is missing.')
        city_ids = {}
        for city in self.cities:
            files_city_origianl_all = glob(f'{self.data_path}data_cumulative/city_date/{city}/original/*')
            files_city_original = keep_files_within_x_days_old(files_city_origianl_all, 
                self.process_datetime, days = self.process_days, date_prefix ='records_')
            tmp_ids = []
            for file in files_city_original:
                if self.verbose: print('reading ids from ' + file)
                ids = get_a_var_from_jsonfile('id', file)
                tmp_ids.append(ids)
            city_ids[city] = list(pd.concat(tmp_ids, ignore_index=True)) if len(tmp_ids)>0 else []
        return city_ids

    @cached_property    
    def city_df(self):
        city_df = {}
        for city in self.cities: city_df[city] = []
        
        for file in self.files:
            if self.verbose: print('loading ' + file)          
            df_file = self.df_reader(file, **self.df_reader_args)
            convert_floats(df_file, target_dtype=self.float_dtype, exceptions=['id', 'RT_id'])

            for city in self.cities:
                if self.verbose: print('gathering data for ' + city) 
                filtered_df = self.city_df_filter(self, df_file, city)
                if len(filtered_df): city_df[city].append(filtered_df)
        return city_df   
  

def city_df_filter_by_filterwords(DataByCityDate, df_file, city):
    # note: used by 'retweet' data_type for a given city
    filterword = DataByCityDate.city_filterwords[city]    
    idx = mark_var_containing_filterwords(df_file, 'tokens', filterword)
    return df_file[idx]

def city_df_filter_by_RT_id(DataByCityDate, df_file, city):
    # note: used by 'retweet' data_type for all_cities 
    return keep_if_var_in_valuelist(df_file, 'RT_id', DataByCityDate.city_RT_ids[city])
 
def city_df_filter_by_filterwords_or_RT_id(DataByCityDate, df_file, city):
    # note: used by 'original' data_type for a given city
    filter_word = DataByCityDate.city_filterwords[city]
    idx_filter_word = mark_var_containing_filterwords(df_file, 'tokens', filter_word)
    idx_RT_flag = mark_var_in_valuelist(df_file, 'RT_id', DataByCityDate.city_RT_ids.get(city,[]))
    idx_keep = [i1 or i2 for i1, i2 in zip(idx_filter_word, idx_RT_flag)]
    return df_file[idx_keep]

def city_df_filter_by_sample(DataByCityDate, df_file, city=None):
    # note: used by 'original' data_type for all_cities 
    return df_file.sample(frac=DataByCityDate.sample_frac, replace=False)

def city_df_filter_by_id(DataByCityDate, df_file, city):
    # note: used by 'sentiments', 'emotions', and 'words' data_type 
    tmp_df = keep_if_var_in_valuelist(df_file, 'id', DataByCityDate.city_ids.get(city,[]))
    if DataByCityDate.verbose: print(f'matched data for {city}: {str(len(tmp_df))} records')  
    return tmp_df



def mark_var_containing_filterwords(df, varname, filterwords):
    filterwords = list(filterwords) 
    idx = {}
    for word in filterwords:
        if type(word)==str:
            idx[word] = df[varname].apply(lambda x: word.lower() in remove_punct_from_tokens(x))
        elif type(word)==list:
            # assess whether all components of 'word' are jointly present 
            idx[str(word)] = df[varname].apply(lambda x: all(w.lower() in remove_punct_from_tokens(x) for w in word))
        else: raise ValueError('each item in filterwords must be str or list')
        # assess whether any component of 'filterwords' are present 
    return apply_fun_to_dict_data(idx, max, axis=1)




