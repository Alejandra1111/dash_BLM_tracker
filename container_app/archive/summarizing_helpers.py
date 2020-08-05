import numpy as np
import pandas as pd
from datetime import datetime
from collections import Counter


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
    for emo in [*emo_labels]:
        df_emotions[emo] = pd.to_numeric(df_emotions[emo])

    stat = (df_emotions[['created_at_h', *emo_labels]]
     .groupby('created_at_h')
     .agg('mean')
     .round(3)
     .sort_values("created_at_h") 
     .reset_index()
    )

    stat = stat.rename(columns ={'created_at_h':'time'})
    return stat
 



def get_cum_times(now=None):
    if now is None:
        now = datetime.utcnow() + pd.DateOffset(hours=-6)
    
    now_1h = now + pd.DateOffset(hours=-1)
    today = pd.to_datetime(now.strftime('%Y-%m-%d %H:%M:%S')).floor("d")
    yesterday = today + pd.DateOffset(days=-1)
    seven_d_ago = today + pd.DateOffset(days=-7)    
    #print('now: ', now.strftime('%Y-%m-%d %H:%M:%S'))
    #print('now_1h: ', now_1h.strftime('%Y-%m-%d %H:%M:%S'))
    #print('today: ', today.strftime('%Y-%m-%d'))
    #print('yersterday: ', yesterday.strftime('%Y-%m-%d'))
    #print('seven_d_ago: ', seven_d_ago.strftime('%Y-%m-%d'))
    return now, now_1h, today, yesterday, seven_d_ago


#get_cum_times()
#get_cum_times(datetime(2020, 6, 28, 21, 00, 00))

class placeholder():
    def __init__(self):
        return
    
def time_subsets(df, now=None):
  print('IN time_subsets():')
  now, now_1h, today, yesterday, seven_d_ago = get_cum_times(now)
  #df.created_at_h = pd.to_datetime(df.created_at_h)
  df['created_at_d'] = df.created_at_h.dt.floor('d')
  subsets = placeholder()
  subsets.now_1h = df[df.created_at_h == now_1h]
  subsets.today = df[df.created_at_d== today]
  subsets.yesterday = df[df.created_at_d== yesterday]
  subsets.seven_days = df[(df.created_at_d <= today) & (df.created_at_d >=seven_d_ago)].reset_index()
  return(subsets)

def calc_stat_words(df_words):
    print('IN calc_stat_words():')
    if df_words is None: return pd.DataFrame(data = {'token_counter':[{}], 'count':[0]})

    df_words = df_words.dropna(subset=['token_counter'])

    # This may take some time
    stat = (#df_words[['created_at_h', 'token_counter']]
        df_words[['token_counter', 'count']]
     #.groupby('created_at_h') # don't need to group
    # .agg(['sum','count'])
    .agg(['sum'])
     #.sort_values("created_at_h") 
     #.reset_index()
    )

    #stat.columns = stat.columns.droplevel(level = 0)
    #stat.columns = ['time','token_counter','count']

    return stat 


def df_words_sample(df_words, butch_num = 100, num_to_sample = 10000):
    ''' 
    prepares data for word cloud as a random sample of data  
    ''' 
    #df_words.token_counter = df_words.token_counter.apply(lambda x: Counter(x))  
    if len(df_words) == 0: return None

    df_words = df_words.dropna(subset=['token_counter'])[['token_counter']] #[['created_at_h', 'token_counter']]

    if len(df_words) < num_to_sample:
        df = df_words
    else:
        df = df_words.sample(n=num_to_sample)

    rep = np.floor((len(df)/butch_num))
    last = len(df) % butch_num
    idx_beg = 0
    idx_end = butch_num

    agg_words = []

    i = 0
    while i <= rep:
        #print('aggregating rows in df_words: ', idx_beg, idx_end)

        a = (df[idx_beg:idx_end]
         #.groupby('created_at_h')
         .agg(['sum', 'count'])
         #.sort_values("created_at_h")
        )
        #a.columns = a.columns.droplevel(level = 0)
        #a = a.reset_index().rename(columns = {'sum':'token_counter'})
        a = a.transpose().rename(columns = {'sum':'token_counter'})

        # Counter(dict())
        # reduce the token_counter to the most common 200 words
        a['token_counter'] = a.token_counter.apply(lambda x: x.most_common(200))
        agg_words.append(a)

        i += 1
        idx_beg += butch_num
        if (i== rep-1): 
            if last==0:
                break
            else:
                idx_end += last
        else:
            idx_end += butch_num

    df_agg_words = pd.concat(agg_words, ignore_index=True)
    df_agg_words['token_counter'] = df_agg_words.token_counter.apply(lambda x: Counter(dict(x)))
    return df_agg_words
    
#tmp = df_words_sample(cum_data.cum_words.now_1h)
#tmp

def get_top_tweets(ori_data, cum_rt_data, subset_name=''):
    print('IN get_top_tweets():')
    df_null = pd.DataFrame(
      data = {'subset':[subset_name], 'RT_id':[''],
      'user_name':[''], 'followers_count':[''], 'text':[''], 't_co':[''],
       'tags':[''], 'retweet_timespan':[''], 'retweet_total':['']}
      )
    if len(ori_data)==0: return df_null 
    len_top = max(15, len(ori_data))

    top15_TW = ori_data.RT_id.value_counts()[:(len_top + 1)]
    top15_TW = top15_TW[top15_TW.index!=''][:len_top]
    top15_TW.index = top15_TW.index.astype(str)
    # print(top15_TW)

    top_RT = cum_rt_data[['RT_id','user_name','followers_count','text','t_co','tags']]
    top_RT.RT_id = top_RT.RT_id.astype(str)

    idx1 = [id in top15_TW.keys() for id in top_RT.RT_id]
    if sum(idx1)==0: return df_null
    top_RT = top_RT[idx1].drop_duplicates(subset=['RT_id'])

    top_TW_count = ori_data[['RT_id','RT_retweet_count']][ori_data.RT_id != '']
    idx2 = [id in top15_TW.keys() for id in top_TW_count.RT_id]

    top_TW_count = (top_TW_count.loc[idx2]
                    .groupby('RT_id')
                    .agg(['count', 'max'])
                    .reset_index()
                   )
    top_TW_count.columns = top_TW_count.columns.droplevel(level=0)
    top_TW_count = top_TW_count.rename(columns={'':'RT_id','count':'retweet_timespan','max':'retweet_total'})

    top_tweets = (top_RT.set_index('RT_id')
     .join(top_TW_count.set_index('RT_id'))
     .sort_values(by=['retweet_timespan'], ascending=False)
     .reset_index()
    )
    if (subset_name != ''):
        cols = top_tweets.columns
        top_tweets['subset'] = subset_name 
        top_tweets = top_tweets[['subset', *cols]]
    return top_tweets


#tmp = get_top_tweets(cum_data.cum_ori.now_1h, cum_data.cum_rt, subset_name='now_1h')
#tmp


def get_top_users(ori_data, cum_rt, subset_name=''):
    print('IN get_top_users():')
    df_null = pd.DataFrame(
      data = {'subset':[subset_name], 'user_id':[''],
       'index':[''], 'RT_id':[''], 'user_name':[''], 'user_description':[''],
       'followers_count':[''], 'following_count':[''], 'retweeted':['']}
      )
    if len(ori_data)==0: return df_null
    ori_data = ori_data[['RT_id']][ori_data.RT_id !=''] 

    merged = (ori_data.set_index('RT_id')  
           .join(cum_rt[['RT_id','user_id','user_name','user_description',
                            'followers_count','following_count']].set_index('RT_id'),
                rsuffix='_RT')
          .dropna(subset=['user_id'])
          )
    merged.user_id = merged.user_id.astype(int).astype(str)
    len_top = max(15, len(merged))
    top15_users = merged.user_id.value_counts()[:len_top]
    # print(top15_users)

    idx1 = [id in [*top15_users.keys()] for id in merged.user_id]  
    if sum(idx1)==0: return df_null

    merged2 = (merged[idx1].reset_index()
     .sort_values(by=['followers_count','RT_id'], ascending=False)
     .drop_duplicates(subset = 'user_id')
     .reset_index()
    )
    
    top_users = (merged2.set_index('user_id')
          .join(top15_users)
          .rename(columns = {'user_id':'retweeted'})
          .reset_index()
          .sort_values(by = 'retweeted', ascending = False)
         )
    
    if (subset_name != ''):
        cols = top_users.columns
        top_users['subset'] = subset_name 
        top_users = top_users[['subset', *cols]]
        
    return top_users

#tmp = get_top_users(cum_data.cum_ori.now_1h, cum_data.cum_rt, subset_name='now_1h')
#tmp

class cumulative_data():
    def __init__(self, cum_ori, cum_rt, cum_words, now=None):
        self.cum_ori = time_subsets(cum_ori, now=now)
        self.cum_rt = cum_rt
        self.cum_words = time_subsets(cum_words, now=now)
        
    def add_words_subsets(self):
        print('IN add_words_subsets():')
        subs = self.cum_words
        print(subs.now_1h)
        # try:
        stat_words = (calc_stat_words(df_words_sample(subs.now_1h)).
                      append(calc_stat_words(df_words_sample(subs.today))).
                      append(calc_stat_words(df_words_sample(subs.yesterday))).
                      append(calc_stat_words(df_words_sample(subs.seven_days)))
                     )
        print(stat_words)
        stat_words.index = ['now_1h', 'today', 'yesterday', 'seven_days']
        # except Exception as e:
            # print(e.__doc__)
        self.stat_words = stat_words.reset_index().rename(columns = {'index': 'subset'})
        
    def add_tweet_subsets(self):
        print('IN add_tweet_subsets():')
        subs = self.cum_ori
        cum_rt = self.cum_rt
        #try:
        top_tweets = (get_top_tweets(subs.now_1h, cum_rt, subset_name='now_1h')
                      .append(get_top_tweets(subs.today, cum_rt, subset_name='today'))
                      .append(get_top_tweets(subs.yesterday, cum_rt, subset_name='yesterday'))
                      .append(get_top_tweets(subs.seven_days, cum_rt, subset_name='seven_days'))
                     )
        print(top_tweets)
       # except Exception as e:
       #     print(e.__doc__)
        self.top_tweets = top_tweets
        
    def add_user_subsets(self):
        print('IN add_user_subsets():')
        subs = self.cum_ori
        cum_rt = self.cum_rt
       # try:
        top_users = (get_top_users(subs.now_1h, cum_rt, subset_name='now_1h')
                      .append(get_top_users(subs.today, cum_rt, subset_name='today'))
                      .append(get_top_users(subs.yesterday, cum_rt, subset_name='yesterday'))
                      .append(get_top_users(subs.seven_days, cum_rt, subset_name='seven_days'))
                     )
        print(top_users)
       # except Exception as e:
       #     print(e.__doc__)
        self.top_users = top_users
           

