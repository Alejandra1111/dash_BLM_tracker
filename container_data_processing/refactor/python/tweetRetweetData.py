from copy import deepcopy
import pandas as pd
from cached_property import cached_property


from utilities import time_now_pandas, extract_hour_from_pandas_timestamp, \
     make_var_pandas_timestamp, PlaceHolder
from tweetRetweetStats import * 
from globals import stat_days_short, stat_days_long


def get_subset_times(now=None):
    if now is None:
        now = time_now_pandas(tzname='America/Denver')
    
    now_1h = now + pd.DateOffset(hours=-1)
    today = now.floor("d")
    yesterday = today + pd.DateOffset(days=-1)
    seven_days = today + pd.DateOffset(days=-stat_days_short)  # note: 'seven_days' = stat_days_short
    return now, now_1h, today, yesterday, seven_days

def create_time_subsets_of_df(df, timevar='created_at_h', now=None):
    now, now_1h, today, yesterday, seven_days = get_subset_times(now)
    
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
    subsets.seven_days = df[(df._timevar_d <= today) & (df._timevar_d >=seven_days)]
    return subsets

    
class TweetRetweetData:
    def __init__(self, cum_ori, cum_rt, cum_words, now=None, process_hours=[]):
        self.cum_ori = create_time_subsets_of_df(cum_ori, timevar='created_at_h', now=now)
        self.cum_rt = cum_rt
        self.cum_words = create_time_subsets_of_df(cum_words, timevar='created_at_h', now=now)
        self.now = now
        self.subset_names = ['today', 'yesterday', 'seven_days'] + [f'hour_{str(h)}' for h in range(24)]
        if 0 < len(process_hours) < 24: 
            self.subset_names =  ['today', 'yesterday', 'seven_days'] + [f'hour_{str(h)}' for h in process_hours]

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
