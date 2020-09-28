from glob import glob
import numpy as np
import pandas as pd
import os
import json
from collections import Counter

class DfWords():
    ''' prepares word count related data for word cloud and word frequency statistics
    '''
    def __init__(self, df, butch_num = 100, num_to_sample = 10000, num_most_common=200, use_df_agg=True):

        self.df = df
        self.use_counter()
        self.butch_num = butch_num
        self.num_to_sample = num_to_sample
        self.num_most_common = 200
        self.use_df_agg = use_df_agg

    def use_counter(self):
        if not hasattr(self.df, 'token_counter'):
            raise TypeError('Wrong data frame type or missing "token_counter" attribute.')
        if type(self.df.iloc[0,:].token_counter) != type(Counter()):
            self.df.token_counter = self.df.token_counter.apply(lambda x: Counter(x))  


    def df_words_sample(self):
        ''' takes a random sample data, aggregates token_coutner at the butch level
        ''' 
        df = self.df
        butch_num = self.butch_num
        df = df.dropna(subset=['token_counter'])[['token_counter']] #[['created_at_h', 'token_counter']]

        if len(df)==0: return pd.DataFrame(columns=['token_counter','count'])

        if len(df) < self.num_to_sample:
            df = df
        else:
            df = df.sample(n=self.num_to_sample)

        rep = np.floor((len(df)/butch_num)) + 1
        num_last_butch = len(df) % butch_num
        idx_beg = 0
        idx_end = butch_num

        agg_words = []

        i = 0
        while i <= rep:
            a = (df[idx_beg:idx_end]
             .agg(['sum', 'count'])
            )

            if a.loc['count'][0]==0: break
            a = a.transpose().rename(columns = {'sum':'token_counter'})

            # reduce the token_counter to the most common 200 words
            a['token_counter'] = a.token_counter.apply(lambda x: x.most_common(self.num_most_common))
            agg_words.append(a)

            i += 1
            idx_beg += butch_num
            if i== rep-1: 
                if num_last_butch==0:
                    break
                else:
                    idx_end += num_last_butch
            else:
                idx_end += butch_num

        df_agg_words = pd.concat(agg_words, ignore_index=True)
        df_agg_words['token_counter'] = df_agg_words.token_counter.apply(lambda x: Counter(dict(x)))
        self.df_agg = df_agg_words


    def calc_stat_words(self):
        ''' aggregates token_coutner for the whole dataset, which is either self.df or self.df_agg
        ''' 
        if self.use_df_agg:
            self.df_words_sample()
            df = self.df_agg
        else:
            df = self.df

        df = df.dropna(subset=['token_counter'])

        if len(df)==0: return pd.DataFrame(data={'token_counter':[{}],'count':[0]})
        
        self.stat = df[['token_counter', 'count']].agg(['sum'])
        self.word_set =  list(self.stat.token_counter[0].keys())
    

 
class InvIndex(DfWords):
    ''' Used to generate an inverted index dictionary containing for keyword search
    '''
    def __init__(
        self, filename, max_words=3000, butch_num = 100,
        num_to_sample = 10000, num_most_common=200, use_df_agg=True):
        df = pd.read_json(filename, orient='records', lines=True)
        df = df.dropna(subset=['token_counter'])
        df = df[df.token_counter.apply(lambda x: x!={})]
        self.df = df
        self.use_counter()
        self.max_words = max_words
        self.butch_num = butch_num
        self.num_to_sample = num_to_sample
        self.num_most_common = 200
        self.use_df_agg = use_df_agg

    def get_word_set(self):
        ''' Generates a list of all words in self.df.token_counter
        '''
        words = []
        for doc in self.df.token_counter.to_list():
            tokens = doc.keys()
            if tokens:
                words += list(tokens)
        self.word_set = list(set(words))

    def get_inv_idx(self):
        ''' Generates an inverted index dictionary: key=word, value=ids
        '''
        inv_idx = {}
        for word in self.word_set[:self.max_words]:    
            idx = self.df.id[self.df.token_counter.apply(lambda x: word in x)]
            inv_idx[word] = list(idx)
        self.inv_idx = inv_idx



if __name__=='__main__':
    
    path = '/Users/kotaminegishi/big_data_training/python/dash_BLM/new_data/app_data/data_cumulative/city_date/all_v1/'
    file_words =  glob(path + 'words/*')

    path_out = path + 'wordindex/'
    dir_exist = os.path.isdir(path_out)

    if not dir_exist:
        os.mkdir(path_out) 
        print('path created: ' + path_out)

    for file in file_words[:3]:
        print('processing ' + file.split('city_date/')[1])
        df_words = InvIndex(
            file, max_words=3000, butch_num = 1000,
            num_to_sample = 10000, num_most_common=1000, use_df_agg=True)

        df_words.calc_stat_words()
        df_words.get_inv_idx()

        outfile = path + 'wordindex/records_' + file.split('records_')[1]

        with open(outfile, 'w') as f:
            json.dump(df_words.inv_idx, f)






