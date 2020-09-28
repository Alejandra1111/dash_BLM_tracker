import pandas as pd 
from cached_property import cached_property

from utilities import  convert_vars_to_numeric, make_var_counter, \
    convert_vars_to_str, remove_null_on_str_var, get_top_counts_cases, \
    drop_duplicates_index, bring_a_column_first
from batchProcessDf import * 

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
    df['subset'] = subset_name
    return bring_a_column_first(df, 'subset')


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
        top_RT_ids_data_in_retweet = [id for id in self.top_RT_ids.index if id in list(self.cum_rt_data.RT_id)]
        top_RT_data = (self.cum_rt_data[['RT_id','user_name','followers_count','text','t_co','tags']]
                       .set_index('RT_id')
                       .loc[top_RT_ids_data_in_retweet]
                       )
        if len(top_RT_data)==0: return None
        top_RT_data = drop_duplicates_index(top_RT_data, keep='last') 
        return top_RT_data
    
    @cached_property
    def top_RT_stats(self):
        top_RT_stats = (self.ori_data[['RT_id','RT_retweet_count']]
                        .set_index('RT_id')
                        .loc[list(self.top_RT_ids.index)]
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
            self.ori_data[['RT_id']].set_index('RT_id').loc[list(self.top_RT_ids.index)]  
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
            self.userdata_of_top_RT.set_index('user_id').loc[list(self.top_users_ids.index)]
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
    

    
    

