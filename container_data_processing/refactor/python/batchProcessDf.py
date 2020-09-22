import numpy as np 
import pandas as pd 
from collections import Counter
from copy import deepcopy


from utilities import convert_vars_to_numeric, is_xth_column_counter, \
    make_var_counter


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




