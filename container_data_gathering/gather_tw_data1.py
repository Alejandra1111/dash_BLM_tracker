import boto3
import json
import tweepy as tw
import re
# import datetime
from config_tw_data import *


class StreamListenerOriginal(tw.StreamListener):
    def __init__(self, api, stream_name, num_tweets_to_grab=10, rt_minimum=100): 
        self.api = api
        self.counter = 0
        self.num_tweets_to_grab = num_tweets_to_grab
        self.rt_minimum = rt_minimum
        self.stream_name = stream_name

    def on_status(self, status):
        try:
            # focus only on lang = 'en'
            if status.lang != 'en':
                return True
            
            # print('\n', self.counter)    
            if self.counter % 1000 ==0:
                print("Tweet counter: ",self.counter)
                
            # Get retweet flag and id
            if hasattr(status, 'retweeted_status'):
                is_retweet = True
                RT_id = status.retweeted_status.id_str 
                RT_retweet_count = status.retweeted_status.retweet_count
            else:
                is_retweet = False
                RT_id = ''
                RT_retweet_count = 0
            
            # Use full_text for original tweets
            if (not is_retweet):
                if hasattr(status, 'extended_tweet'):
                    text = status.extended_tweet["full_text"]
                else:
                    text = status.text 
            else:
                text = ''
            
            # Obtain retweet text if RT_retweet_count meets a threshold (won't be stored in retweet dataset)
            if ((is_retweet) & (RT_retweet_count < self.rt_minimum)):
                if hasattr(status.retweeted_status, 'extended_tweet'):
                    RT_text = status.retweeted_status.extended_tweet['full_text']
                else:
                    RT_text = status.retweeted_status.text
            else:
                RT_text = ''
            
            # Flag retweet with comments
            if hasattr(status, 'quoted_status'):
                if hasattr(status.quoted_status, 'extended_tweet'):
                    quoted_text = status.quoted_status.extended_tweet['full_text']
                else: 
                    quoted_text = status.quoted_status.text
            else:
                quoted_text = ''
            
            t_co = re.findall('http[s]?://t.co/(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',
                       text + quoted_text)
            tags = re.findall('#(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',
                              text + quoted_text)
            urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',
                              text + quoted_text)
            urls = [i for i in urls if i not in t_co] 

            
            selected_data = {
                'id': str(status.id_str), 'created_at': str(status.created_at), 
                'is_retweet': is_retweet, 'RT_id': str(RT_id), 'RT_retweet_count': RT_retweet_count, 
                'user_id': str(status.user.id_str), 'user_name': status.user.screen_name, 
                'followers_count': status.user.followers_count, 'following_count': status.user.friends_count,
                'text': text, 'quoted_text': quoted_text, 'RT_text': RT_text,
                't_co':t_co , 'tags': tags, 'urls': urls, 'lang': status.lang
            } 
            # print('selected_data: ',json.dumps(selected_data),'\n')
            
      
            response = client.put_record(
                StreamName=self.stream_name,
                Data = bytes(json.dumps(selected_data) + '\n', 'utf-8'),
                PartitionKey='1')
            
            # print('data sent to aws client!')

            self.counter += 1
            
            if self.num_tweets_to_grab > 0: # negative means running continuously 
	            if self.counter >= self.num_tweets_to_grab:
	                print('\nReached the num_tweets_to_grab!')
	                return False
            
            return True
        
        except:
            print('Error in data processing original tweets: (', status,')')
            return False
        
    def on_error(self, status):
        print('Encountered error: (', status,')')
        return False


class StreamListenerRetweet(tw.StreamListener):

    def __init__(self, api, stream_name, num_tweets_to_grab=10, rt_minimum = 100): 
        self.api = api
        self.counter = 0
        self.num_tweets_to_grab = num_tweets_to_grab
        self.rt_minimum = rt_minimum
        self.session_RT_id_list = []
        self.stream_name = stream_name

    def on_status(self, status):
        try:
            # focus only on lang = 'en'
            if status.lang != 'en':
                return True
                               
            # Get retweet id and count
            if hasattr(status, 'retweeted_status'):
                is_retweet = True
                RT_id = status.retweeted_status.id_str 
                retweet_count = status.retweeted_status.retweet_count
                if hasattr(status, 'quoted_status'):
                    is_quoted = True
                else:
                    is_quoted = False
            else: 
                is_retweet = False
                RT_id = None
                retweet_count = None 
            
            #print(RT_id, retweet_count)
                
            # Check if RT_id has already occured in the session and retweet_count meets a threshold
            if is_retweet:
                RT_id_check = ((RT_id not in self.session_RT_id_list) & 
                               (retweet_count >= self.rt_minimum))
            else:
                return True
                          
            if (not RT_id_check):
                return True
            
            # the rest is for RT_id_check = True
            # print('\n', self.counter)    
            if self.counter % 100 ==0:
                print("Retweet counter: ", self.counter) 

            self.session_RT_id_list.append(RT_id)
            #print(self.session_RT_id_list)

            # Get retweet data
            created_at = str(status.retweeted_status.created_at)
            user_id = status.retweeted_status.user.id_str
            user_name = status.retweeted_status.user.screen_name
            followers_count = status.retweeted_status.user.followers_count
            following_count = status.retweeted_status.user.friends_count
            user_description = status.retweeted_status.user.description

            # Use full_text for retweets
            if hasattr(status.retweeted_status, 'extended_tweet'):
                text = status.retweeted_status.extended_tweet['full_text']
            else:
                text = status.retweeted_status.text            

            # Get t_co link to the retweet source (not 100% successful..)
            #print('getting t_co..')
            t_co = []
            if hasattr(status.retweeted_status, 'entities'):
                #print('has entities: ', status.retweeted_status.entities)
                if (status.retweeted_status.entities['urls'] !=[]):
                    #print('has urls: ', status.retweeted_status.entities['urls'])
                    t_co = status.retweeted_status.entities['urls'][0]['url']
                elif len(t_co)==0: 
                    try: 
                        #print('has media', status.retweeted_status.entities['media'])
                        t_co = status.retweeted_status.entities['media'][0]['url']
                    except:
                        pass   

            if ((is_quoted) & (len(t_co)==0)):
                #print('quoted retweet: ')
                if hasattr(status.quoted_status, 'entities'):
                    #print('has entities: ', status.quoted_status.entities)
                    if (status.quoted_status.entities['urls'] !=[]):
                        #print('has urls: ', status.quoted_status.entities['urls'])
                        t_co = status.quoted_status.entities['urls'][0]['url']
                    elif len(t_co)==0: 
                        try: 
                            #print('has media', status.quoted_status.entities['media'])
                            t_co = status.quoted_status.entities['media'][0]['url']
                        except:
                            pass

            if len(t_co) == 0:
                #print('finding t_co in text..')
                #print(status.retweeted_status)
                t_co = re.findall('http[s]?://t.co/(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',
                   text)
            #print(t_co)

            tags = re.findall('#(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',
                          text)
            urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',
                          text)
            urls = [i for i in urls if i not in t_co] 

            selected_data = {
                'RT_id': str(RT_id), 'created_at': created_at, 
                'user_id': str(user_id), 'user_name': user_name, 
                'followers_count': followers_count, 'following_count': following_count,
                'user_description': user_description,
                'text': text, 
                'retweet_count': retweet_count, 
                't_co': t_co , 'tags': tags, 'urls': urls, 'lang': status.lang
            } 
            #print(json.dumps(selected_data))


            response = client.put_record(
               StreamName = self.stream_name,
               Data = bytes(json.dumps(selected_data) + '\n', 'utf-8'),
               PartitionKey='1')

            self.counter += 1
            
            if self.num_tweets_to_grab > 0: # negative means running continuously 
	            if self.counter >= self.num_tweets_to_grab:
	                print('\nReached the num_tweets_to_grab!')
	                return False
            
            return True
        
        except:
            print('Error in data processing retweet: (', status,')')
            return False
        
    def on_error(self, status):
        print('Encountered error: (', status,')')
        return False



if __name__=="__main__":
	
    client = boto3.client('kinesis', aws_region)

    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tw.API(auth, wait_on_rate_limit = False, 
                 wait_on_rate_limit_notify = True)  


    # print("\n------ Streaming -------:")
    while True: # run continuously until stopped
        try:
            listner1 = StreamListenerOriginal(api, stream_original, num_tweets_to_grab=-1) 
            stream1 = tw.Stream(api.auth, listner1, tweet_mode='extended')

            listner2 = StreamListenerRetweet(api, stream_retweet, num_tweets_to_grab=-1) 
            stream2 = tw.Stream(api.auth, listner2, tweet_mode='extended')

            track_words = ['#BlackLivesMatter','#BlackLivesMatters',
            '#BLACK_LIVES_MATTER','#blm','#blacklivesmatter']
            stream1.filter(track=track_words, is_async=True)
            stream2.filter(track=track_words, is_async=True)

        except Exception as e:
            print('Error in main():')
            print(e.__doc__)



    