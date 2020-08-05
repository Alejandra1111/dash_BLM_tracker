import json
import boto3
from io import BytesIO
import numpy as np
import pandas as pd
import itertools 
from datetime import datetime
from collections import Counter

bucket_name = 'kotasstorage1'
session = boto3.Session()
s3_client = session.client("s3")
s3_resource = boto3.resource('s3')
bucket = s3_resource.Bucket(bucket_name)

data_path = 'app_data/'
sample_n = 50000


def getData(filename):
    f = BytesIO()
    s3_client.download_fileobj(bucket_name, filename, f)
    return f.getvalue()

def getFiles(prefix):
	return [object.key for object in bucket.objects.filter(Prefix=prefix)]


    
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


def fix_datetime(df, timevar='created_at_h'):
    df[timevar] = pd.to_datetime(df[timevar])

def fix_token_counter(df):
    df.token_counter = df.token_counter.apply(lambda x: Counter(x))  

def fix_RT_id(df):
    df.RT_id = df.RT_id.astype(str)

def fix_user_id(df):
    df.user_id = df.user_id.astype(str)    

def convert_floats(df, float_dtype='float32'):
    floats = df.select_dtypes(include=['float64']).columns.tolist()
    df[floats] = df[floats].astype(float_dtype)
    return df


def tw_data_files_to_df_csv(files, float_dtype='float32'):
    '''append and concat data files into a pandas.DataFrame'''
    df = []
    [ df.append(pd.read_csv(BytesIO(getData(file)),encoding='latin-1')) for file in files ]
    df = pd.concat(df, ignore_index=True)
    if float_dtype is None: return df
    return convert_floats(df, float_dtype)

def tw_data_files_to_df_json(files, lines=True, float_dtype='float32'):
    '''append and concat data files into a pandas.DataFrame'''
    df = []
    [ df.append(pd.read_json(getData(file), orient='records', lines=lines)) for file in files ]
    df = pd.concat(df, ignore_index=True)
    if float_dtype is None: return df
    return convert_floats(df, float_dtype)    


null_cum_sentiments = pd.DataFrame(columns=['id', 'created_at_h', 'neg', 'neu', 'pos', 'compound'])
null_cum_emotions = pd.DataFrame(columns=['id', 'created_at_h', 'fear', 'anger', 'anticip', 'trust', 'surprise', 'positive', 'negative', 'sadness', 'disgust', 'joy'])
null_cum_words=pd.DataFrame(columns=['id', 'created_at_h', 'token_counter'])
null_cum_original=pd.DataFrame(columns=['id', 'created_at', 'is_retweet', 'RT_id', 'RT_retweet_count',
       'user_id', 'user_name', 'followers_count', 'following_count', 'text',
       'quoted_text', 'RT_text', 't_co', 'tags', 'urls', 'lang',
       'created_at_h', 'tokens'])
null_cum_retweet=pd.DataFrame(columns=['RT_id', 'created_at', 'user_id', 'user_name', 'followers_count',
       'following_count', 'user_description', 'text', 'retweet_count', 't_co',
       'tags', 'urls', 'lang', 'created_at_h', 'tokens'])

def keep_by_matched_id(df, list_id, varname='id'):
    return (df.set_index(varname)
            .join(pd.DataFrame(data={varname: list_id}).set_index(varname), how='inner')
            .reset_index()
            )
    

def get_stats(cum_original, cum_retweet, cum_words, 
	cum_sentiments, cum_emotions, time=None):
	print('In get_stats()')
	stat_sentiments = calc_stat_sentiments(cum_sentiments)
	stat_emotions = calc_stat_emotions(cum_emotions)
	del cum_sentiments, cum_emotions

	cum_data = cumulative_data2(cum_ori = cum_original, 
	                      cum_rt = cum_retweet,
	                      cum_words = cum_words,
	                      now = time
	                      )
	del cum_original, cum_retweet, cum_words

	cum_data.add_words_subsets()
	cum_data.add_tweet_subsets()
	cum_data.add_user_subsets() 

	stat_words = cum_data.stat_words
	top_tweets = cum_data.top_tweets
	top_users = cum_data.top_users 

	return stat_sentiments, stat_emotions, stat_words, top_tweets, top_users


def filter_data(filter_word, cum_original, cum_retweet, cum_words, 
	cum_sentiments, cum_emotions, time=None):
    # define filtered data of cum_data 
    # filter_word = 'protest'
    print('In filter_data():')
    cum_original2, cum_retweet2, cum_words2 = filter_main(filter_word,
    	cum_original, cum_retweet, cum_words)
    
    # print(cum_words2)
    cum_sentiments2 = filter_sentiments(cum_sent = cum_sentiments, 
                                        ori_filtered = cum_original2)
    cum_emotions2 = filter_sentiments(cum_sent = cum_emotions, 
                                        ori_filtered = cum_original2)    
    
    return get_stats(cum_original2, cum_retweet2, cum_words2, cum_sentiments2, cum_emotions2, time)


def filter_sentiments(cum_sent, ori_filtered):
	print('IN filter_sentiments():')

	sent_labels = [*cum_sent.columns][2:]
	# print(sent_labels)
	cum_sent2 = (cum_sent.set_index('id')
                   .join(ori_filtered.set_index('id'), rsuffix = '_ORI', how='inner')
                   .reset_index()
                   )[['id', 'created_at_h', *sent_labels]]
	return cum_sent2


def filter_main(filter_word, cum_original, cum_retweet, cum_words):
	''' filtered data are sequentially defined for retweet, original, words dataset'''
	print('IN filter_datasets():')
	idx_a =  cum_retweet.tokens.apply(lambda x: filter_word in x)
	cum_retweet2 = cum_retweet[idx_a]
	#print(sum(idx_a))

	idx_b = cum_original.tokens.apply(lambda x: filter_word in x)
	match_b = (cum_original.set_index('RT_id')
	        .join(cum_retweet2.set_index('RT_id'), rsuffix = '_RT', how='inner')
	       )
	cum_original2 = (match_b.reset_index()
	                 .append(cum_original[idx_b])
	                 .drop_duplicates(subset=['id'])
	                 .reset_index()
	                )
	#print(sum(idx_b),len(cum_original2))

	cum_words2 = (cum_words.set_index('id')
		.join(cum_original2.set_index('id'), rsuffix = '_ORI', how='inner')
		.reset_index()
		)

	#print(len(cum_words2))

	return cum_original2, cum_retweet2, cum_words2



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
    
# def time_subsets(df, now=None):
#     now, now_1h, today, yesterday, seven_d_ago = get_cum_times(now)
    
#     #df.created_at_h = pd.to_datetime(df.created_at_h)
#     df['created_at_d'] = df.created_at_h.dt.floor('d')
    
#     subsets = placeholder()
#     subsets.now_1h = df[df.created_at_h==now_1h] 
#     subsets.today = df[df.created_at_d==today]
#     subsets.yesterday = df[df.created_at_d==yesterday]
#     subsets.seven_days = df[(df.created_at_d <= today) & (df.created_at_d >=seven_d_ago)]
#     return(subsets)

def calc_stat_words(df_words):
    df_words = df_words.dropna(subset=['token_counter'])

    if len(df_words)==0: return pd.DataFrame(data={'token_counter':[{}],'count':[0]})
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


def df_words_sample(df_words, batch_num = 100, num_to_sample = 10001):
    ''' 
    prepares data for word cloud as a random sample of data  
    ''' 
    #df_words.token_counter = df_words.token_counter.apply(lambda x: Counter(x))  
    df_words = df_words.dropna(subset=['token_counter'])[['token_counter']] #[['created_at_h', 'token_counter']]

    if len(df_words)==0: return pd.DataFrame(columns=['token_counter','count'])

    if len(df_words) < num_to_sample:
        df = df_words
    else:
        df = df_words.sample(n=num_to_sample)

    rep = np.floor((len(df)/batch_num))
    last = len(df) % batch_num
    idx_beg = 0
    idx_end = batch_num

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
        idx_beg += batch_num
        if (i== rep-1): 
            if last==0:
                break
            else:
                idx_end += last
        else:
            idx_end += batch_num

    df_agg_words = pd.concat(agg_words, ignore_index=True)
    df_agg_words['token_counter'] = df_agg_words.token_counter.apply(lambda x: Counter(dict(x)))
    return df_agg_words
    
#tmp = df_words_sample(cum_data.cum_words.now_1h)
#tmp

def get_top_tweets(ori_data, cum_rt_data, subset_name=''):
    #print('IN get_top_tweets():')
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
    ori_data = ori_data[['RT_id']][ori_data.RT_id !=''] 

    merged = (ori_data.set_index('RT_id')  
           .join(cum_rt[['RT_id','user_id','user_name','user_description',
                            'followers_count','following_count']].set_index('RT_id'),
                rsuffix='_RT')
          .dropna(subset=['user_id'])
          )
    merged.user_id = merged.user_id.astype(int).astype(str)
    top15_users = merged.user_id.value_counts()[:15]

    idx1 = [id in [*top15_users.keys()] for id in merged.user_id]  
    
    if (len(idx1)==0): 
      df_null = pd.DataFrame(columns = 
        ['subset', 'user_id', 'index', 'RT_id', 'user_name', 'user_description',
       'followers_count', 'following_count', 'retweeted'])
      df_null['subset'] = subset_name 
      return df_null

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

# class cumulative_data():
#     def __init__(self, cum_ori, cum_rt, cum_words, now=None):
#         self.cum_ori = time_subsets(cum_ori, now=now)
#         self.cum_rt = cum_rt
#         self.cum_words = time_subsets(cum_words, now=now)
        
#     def add_words_subsets(self):
#         print('IN add_words_subsets():')
#         subs = self.cum_words
#         stat_words = (calc_stat_words(df_words_sample(subs.now_1h)).
#                       append(calc_stat_words(df_words_sample(subs.today))).
#                       append(calc_stat_words(df_words_sample(subs.yesterday))).
#                       append(calc_stat_words(df_words_sample(subs.seven_days)))
#                      )
#         #print(stat_words)
#         stat_words.index = ['now_1h', 'today', 'yesterday', 'seven_days']
#         self.stat_words = stat_words.reset_index().rename(columns = {'index': 'subset'})
        
#     def add_tweet_subsets(self):
#         print('IN add_tweet_subsets():')
#         subs = self.cum_ori
#         cum_rt = self.cum_rt
#         top_tweets = (get_top_tweets(subs.now_1h, cum_rt, subset_name='now_1h')
#                       .append(get_top_tweets(subs.today, cum_rt, subset_name='today'))
#                       .append(get_top_tweets(subs.yesterday, cum_rt, subset_name='yesterday'))
#                       .append(get_top_tweets(subs.seven_days, cum_rt, subset_name='seven_days'))
#                      )
#         #print(top_tweets)
#         self.top_tweets = top_tweets
        
#     def add_user_subsets(self):
#         print('IN add_user_subsets():')
#         subs = self.cum_ori
#         cum_rt = self.cum_rt
#         top_users = (get_top_users(subs.now_1h, cum_rt, subset_name='now_1h')
#                       .append(get_top_users(subs.today, cum_rt, subset_name='today'))
#                       .append(get_top_users(subs.yesterday, cum_rt, subset_name='yesterday'))
#                       .append(get_top_users(subs.seven_days, cum_rt, subset_name='seven_days'))
#                      )
#         #print(top_users)
#         self.top_users = top_users


  
def time_subsets_all(df, now=None):
        now, now_1h, today, yesterday, seven_d_ago = get_cum_times(now)
        
        #df.created_at_h = pd.to_datetime(df.created_at_h)
        df['created_at_d'] = df.created_at_h.dt.floor('d')
        df['hour'] = int(str(df.created_at_h)[11:13])

        subsets = placeholder()
        for h in range(24):
        	setattr(subsets,'hour_' + str(h), df[df.hour==h]) 

        subsets.today = df[df.created_at_d==today]
        subsets.yesterday = df[df.created_at_d==yesterday]
        subsets.seven_days = df[(df.created_at_d <= today) & (df.created_at_d >=seven_d_ago)]
        return(subsets)


class cumulative_data2():
    def __init__(self, cum_ori, cum_rt, cum_words, now=None):
        self.cum_ori = time_subsets_all(cum_ori, now=now)
        self.cum_rt = cum_rt
        self.cum_words = time_subsets_all(cum_words, now=now)
        self.now = now

    def add_words_subsets(self):
        subs = self.cum_words

        stat_words = (calc_stat_words(df_words_sample(subs.today)).
                      append(calc_stat_words(df_words_sample(subs.yesterday))).
                      append(calc_stat_words(df_words_sample(subs.seven_days)))
                     )
       	
        for h in range(24):
        	stat_words = stat_words.append(
        		calc_stat_words(df_words_sample(getattr(subs,'hour_' + str(h))))
        		)

       	stat_words.index = ['today', 'yesterday', 'seven_days'] + ['hour_' + str(h) for h in range(24)]
        self.stat_words = stat_words.reset_index().rename(columns = {'index': 'subset'})
        
    def add_tweet_subsets(self):
        subs = self.cum_ori
        cum_rt = self.cum_rt

        top_tweets = (get_top_tweets(subs.today, cum_rt, subset_name='today')
                  .append(get_top_tweets(subs.yesterday, cum_rt, subset_name='yesterday'))
                  .append(get_top_tweets(subs.seven_days, cum_rt, subset_name='seven_days'))
                 )

        for h in range(24):
        	hour_h = 'hour_' + str(h)
        	top_tweets = top_tweets.append(
        		get_top_tweets(getattr(subs, hour_h), cum_rt, subset_name=hour_h)
        	)
        self.top_tweets = top_tweets
        
    def add_user_subsets(self):
        subs = self.cum_ori
        cum_rt = self.cum_rt
        
    def add_user_subsets(self):
        subs = self.cum_ori
        cum_rt = self.cum_rt

        top_users = (get_top_users(subs.today, cum_rt, subset_name='today')
                      .append(get_top_users(subs.yesterday, cum_rt, subset_name='yesterday'))
                      .append(get_top_users(subs.seven_days, cum_rt, subset_name='seven_days'))
                     )
        for h in range(24):
        	hour_h = 'hour_' + str(h)
        	top_users = top_users.append(
        		get_top_users(getattr(subs, hour_h), cum_rt, subset_name=hour_h)
        		)
        self.top_users = top_users
        
           


def lambda_handler(event, context):
	city = event['city']
	date = event['date']
	filter_keyword = event['filter_keyword']
	#hour = event['hour']

	base_timestamp = pd.to_datetime(date + ' ' + '23:59')

	if city is None: return None

	print(data_path + "data_cumulative/city_date/" + city)
	cum_data_path = data_path + "data_cumulative/city_date/" + city
	curr_data_path = data_path + "data_current/city/" + city
	datasets = {}

	print('  Loading cumulative data: original tweets and retweets...')   
	# load recent cumulative data     
	files_original = keep_recent_files(getFiles(cum_data_path + "/original/records_"),
	    base_timestamp = base_timestamp, prefix='records_', 
	    file_type = '.json', days=8, no_newer=True)
	if len(files_original)>0:
	    cum_original = tw_data_files_to_df_json(files_original, lines=True)
	    #fix_datetime(cum_original)        
	    #fix_RT_id(cum_original)
	    print(cum_original.head())
	    print(len(cum_original))
	    cum_original = cum_original.drop(columns=['created_at','following_count','lang','urls'])
	    take_sample = True if len(cum_original)>sample_n else False
	    if take_sample: cum_original = cum_original.sample(n=sample_n, replace=False)
	else:
	    cum_original = null_cum_original
	    take_sample = False
	#datasets['cum_original']=cum_original.to_json(orient='split')

	files_retweet = cum_data_path + "/retweet/2020_all_retweets.json"
	try: 
	    cum_retweet = pd.read_json(getData(files_retweet), orient='records', lines=True)
	    #fix_datetime(cum_retweet)
	    #fix_RT_id(cum_retweet)
	except:
	    cum_retweet = null_cum_retweet

	#datasets['cum_retweet']=cum_retweet.to_json(orient='split')

	if take_sample:
		keep_ids = list(cum_original.id.append(cum_retweet.RT_id.rename('id')).drop_duplicates())

	#del cum_original, cum_retweet

	# load recent cumulative data
	print('  Loading cumulative data: sentiments and emotions...')

	files_sentiments = keep_recent_files(getFiles(cum_data_path + "/sentiments/records_"),
	                    base_timestamp=base_timestamp, prefix='records_',
	                    file_type = '.csv', days=15, no_newer=True) 
	print(files_sentiments)
	if len(files_sentiments)>0: 
	    cum_sentiments = tw_data_files_to_df_csv(files_sentiments)
	    cum_sentiments = cum_sentiments[cum_sentiments.compound.isnull()==False].drop_duplicates(subset = 'id')
	    #fix_datetime(cum_sentiments)
	else:
	    cum_sentiments = null_cum_sentiments
	    
	files_emotions = keep_recent_files(getFiles(cum_data_path + "/emotions/records_"),
	                    base_timestamp=base_timestamp, prefix='records_',
	                    file_type = '.csv', days=15, no_newer=True) 
	if len(files_emotions)>0:
	    cum_emotions = tw_data_files_to_df_csv(files_emotions)
	    cum_emotions = cum_emotions[cum_emotions.fear.isnull()==False].drop_duplicates(subset = 'id')
	    #fix_datetime(cum_emotions)    
	else:
	    cum_emotions = null_cum_emotions

	#datasets['cum_sentiments']=cum_sentiments.to_json(orient='split')
	#datasets['cum_emotions']=cum_emotions.to_json(orient='split')
	 
	#del cum_emotions, cum_sentiments

	print('  Loading cumulative data: words...')
	files_words = keep_recent_files(getFiles(cum_data_path + "/words/records_"),
	                                base_timestamp=base_timestamp, prefix='records_',
	                                file_type = '.json', days=8, no_newer=True) 

	if len(files_words)>0:
	    cum_words = tw_data_files_to_df_json(files_words, lines=True)
	    cum_words =cum_words[(cum_words.token_counter!={}) & (cum_words.token_counter.isnull()==False)]
	    #fix_datetime(cum_words)
	    #fix_token_counter(cum_words)
	    if take_sample:
	    	print('len cum_words 0:', len(cum_words))
	    	cum_words = keep_by_matched_id(cum_words, keep_ids)
	    	print('len cum_words 1:', len(cum_words))
	else:
	    cum_words = null_cum_words

	#datasets['cum_words'] = cum_words.to_json(orient='split')
	#del cum_words

	# datasets = {
	#      'cum_original': cum_original.to_json(orient='split'),
	#      'cum_retweet': cum_retweet.to_json(orient='split'),
	#      'cum_words': cum_words.to_json(orient='split'),
	#      'cum_sentiments': cum_sentiments.to_json(orient='split'),
	#      'cum_emotions': cum_emotions.to_json(orient='split')
	#  }
	# correct data types
	fix_datetime(cum_sentiments, timevar='created_at_h')
	fix_datetime(cum_emotions, timevar='created_at_h')
	fix_datetime(cum_words, timevar='created_at_h')
	fix_token_counter(cum_words)
	fix_datetime(cum_original, timevar='created_at_h')
	fix_datetime(cum_retweet, timevar='created_at_h')
	fix_RT_id(cum_original)
	fix_RT_id(cum_retweet)
	
	print('End of load_city_date_data():')

	print('calculating stats..')
	
	#picked_time = pd.to_datetime(date[:10] + ' ' + hour)
	picked_time = base_timestamp
	print(cum_original.head())
	print(cum_words.head())
	print(cum_emotions.head())
	print(cum_sentiments.head())
	print(cum_retweet.head())
	
	if filter_keyword != '':
		print('filtering data..')
		stat_sentiments, stat_emotions, stat_words, top_tweets, top_users = filter_data(
			filter_keyword, cum_original, cum_retweet, cum_words, cum_sentiments, cum_emotions, time=picked_time)
	else:
	    stat_sentiments, stat_emotions, stat_words, top_tweets, top_users = get_stats(
    		cum_original, cum_retweet, cum_words, cum_sentiments, cum_emotions, time=picked_time)
	del cum_original, cum_retweet, cum_words, cum_sentiments, cum_emotions
	
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
         'time': str(picked_time),
         'type': 'filtered stats'
    }
	return json.dumps(stats)
