
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



class placeholder():
    def __init__(self):
        return


    
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
  
