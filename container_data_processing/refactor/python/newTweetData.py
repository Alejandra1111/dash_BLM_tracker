import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nrclex import NRCLex
from collections import Counter
from copy import deepcopy
from cached_property import cached_property
import pandas as pd

from utilities import data_files_to_df_json, convert_time, add_time_floored_at_hour, \
    keep_if_var_has_positive_length, convert_vars_to_str
from token_utils import *


class NewTweetData():    
    def __init__(self, new_filenames, id_varname, current_time):
        self.id_varname = id_varname
        self.new_filenames = new_filenames
        self.current_time = current_time
        self.df_sentiments, self.df_top_emotions, self.df_count_words = [pd.DataFrame()] * 3
        
    @cached_property
    def df(self): 
        if not self.new_filenames: return pd.DataFrame()
        df = data_files_to_df_json(self.new_filenames, orient='records', lines=True)
        convert_time(df, self.current_time, 'US/Mountain', varname='created_at')
        add_time_floored_at_hour(df, floored_varname='created_at_h')
        self.add_tokens_for_text_and_quoted_text(df)
        return df.drop_duplicates(subset = [self.id_varname])
        
    @cached_property
    def df_with_non_empty_tokens(self): 
        if len(self.df)==0: return pd.DataFrame()
        return keep_if_var_has_positive_length(self.df, 'tokens')

            
    def add_tokens_for_text_and_quoted_text(self, df):
        if hasattr(df, 'quoted_text'):
                df['tokens'] = get_clean_lemmatized_tokens(df.text + df.quoted_text)
        else:
                df['tokens'] = get_clean_lemmatized_tokens(df.text)
        
    def assign_sentiments(self):
        df = self.df_with_non_empty_tokens
        if len(df): 
            sid = SentimentIntensityAnalyzer()
            sentiments = []

            for i, row in df.iterrows():
                score = sid.polarity_scores(join_token(row['tokens']))
                sentiments.append([row[self.id_varname], row['created_at_h']] + [score[key] for key in score])
            self.df_sentiments = pd.DataFrame(sentiments,
                                              columns = [self.id_varname, 'created_at_h'] + list(score.keys()))

    def assign_emotions(self):
        df = self.df_with_non_empty_tokens
        if len(df):
            nrc_1 = NRCLex(join_token(df['tokens'].iloc[0]))
            emo_labels = nrc_1.affect_frequencies.keys()
            top_emotions = []

            for i, row in df.iterrows():
                nrc = NRCLex(join_token(row['tokens']))
                emos = [ i[0] for i in nrc.top_emotions]
                top_emotions.append([row[self.id_varname], row['created_at_h']] + [i in emos for i in emo_labels])
            self.df_top_emotions = pd.DataFrame(top_emotions,
                                                columns = [self.id_varname, 'created_at_h'] + list(emo_labels))

    def count_words(self):
        df = deepcopy(self.df)
        if len(df):
            df['token_counter'] = df['tokens'].apply(lambda x: Counter(x))
            self.df_words = df[[self.id_varname, 'created_at_h', 'token_counter']]

    def assign_sentiments_and_wordcounts(self):
        self.assign_sentiments()
        self.assign_emotions()
        self.count_words()



def append_matching_rt_data(ori_df, ori_data, rt_data):
    if not len(ori_data): return pd.DataFrame()
    col_data = [*ori_data.columns]
    ori_matched = get_matching_rt_data(ori_df, rt_data)
    
    if len(ori_matched):
        df_appended = (
            ori_data
            .append(ori_matched[[*col_data]])
            )
        print('Num rows = original:{} + retweet:{} = total:{}'
          .format(len(ori_data), len(ori_matched), len(df_appended)))
        return df_appended 
   
    else: 
        print('Num rows = original:{}'.format(len(ori_data)))
        return or_data

def get_matching_rt_data(ori_df, rt_data): 
    convert_vars_to_str(rt_data,['RT_id']) 
    ori_to_be_matched = keep_if_var_has_positive_length(ori_df, 'RT_id')
    
    if len(ori_to_be_matched):
        matched_data = (
            ori_to_be_matched[['id','RT_id','created_at_h']]
            .join(rt_data.set_index('RT_id'), 
                   on='RT_id', rsuffix='_rt')
            )
        return matched_data
    else:
        return pd.DataFrame()

   
