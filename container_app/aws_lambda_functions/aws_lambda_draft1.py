import json
import boto3
from io import BytesIO
import pandas as pd
from glob import glob
import itertools 

bucket_name = 'kotasstorage1'
session = boto3.Session()
s3_client = session.client("s3")

data_path = 'app_data/'

def getData(filename):
    f = BytesIO()
    s3_client.download_fileobj(bucket_name, filename, f)
    return f.getvalue()
    
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

def tw_data_files_to_df_json(files, lines=False, float_dtype='float32'):
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

def lambda_handler(event, context):
	city_datetime = event['city_datetime']
	print('In load_city_date_data(%s):' % city_datetime)
	city, base_timestamp = city_datetime.split('#') 
	base_timestamp = pd.to_datetime(base_timestamp)
	if city is None: return None

	print(data_path + "data_cumulative/city_date/" + city)
	cum_data_path = data_path + "data_cumulative/city_date/" + city
	curr_data_path = data_path + "data_current/city/" + city
	datasets = {}

	print('  Loading cumulative data: original tweets and retweets...')   
	# load recent cumulative data     
	files_original = keep_recent_files(glob(cum_data_path + "/original/*"),
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
	datasets['cum_original']=cum_original.to_json(orient='split')

	files_retweet = cum_data_path + "/retweet/2020_all_retweets.json"
	try: 
	    cum_retweet = pd.read_json(getData(files_retweet), orient='records', lines=True)
	    #fix_datetime(cum_retweet)
	    #fix_RT_id(cum_retweet)
	except:
	    cum_retweet = null_cum_retweet

	datasets['cum_retweet']=cum_retweet.to_json(orient='split')

	if take_sample:
		keep_ids = list(cum_original.id.append(cum_retweet.RT_id.rename('id')).drop_duplicates())

	del cum_original, cum_retweet

	# load recent cumulative data
	print('  Loading cumulative data: sentiments and emotions...')

	files_sentiments = keep_recent_files(glob(cum_data_path + "/sentiments/*"),
	                    base_timestamp=base_timestamp, prefix='records_',
	                    file_type = '.csv', days=15, no_newer=True) 
	#print(files_sentiments)
	if len(files_sentiments)>0: 
	    cum_sentiments = tw_data_files_to_df_json(files_sentiments)
	    cum_sentiments = cum_sentiments[cum_sentiments.compound.isnull()==False].drop_duplicates(subset = 'id')
	    #fix_datetime(cum_sentiments)
	else:
	    cum_sentiments = null_cum_sentiments
	    
	files_emotions = keep_recent_files(glob(cum_data_path + "/emotions/*"),
	                    base_timestamp=base_timestamp, prefix='records_',
	                    file_type = '.csv', days=15, no_newer=True) 
	if len(files_emotions)>0:
	    cum_emotions = tw_data_files_to_df_json(files_emotions)
	    cum_emotions = cum_emotions[cum_emotions.fear.isnull()==False].drop_duplicates(subset = 'id')
	    #fix_datetime(cum_emotions)    
	else:
	    cum_emotions = null_cum_emotions

	datasets['cum_sentiments']=cum_sentiments.to_json(orient='split')
	datasets['cum_emotions']=cum_emotions.to_json(orient='split')
	 
	del cum_emotions, cum_sentiments

	print('  Loading cumulative data: words...')
	files_words = keep_recent_files(glob(cum_data_path + "/words/*"),
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

	datasets['cum_words'] = cum_words.to_json(orient='split')
	del cum_words

	# datasets = {
	#      'cum_original': cum_original.to_json(orient='split'),
	#      'cum_retweet': cum_retweet.to_json(orient='split'),
	#      'cum_words': cum_words.to_json(orient='split'),
	#      'cum_sentiments': cum_sentiments.to_json(orient='split'),
	#      'cum_emotions': cum_emotions.to_json(orient='split')
	#  }
	print('Exiting load_city_date_data():')

	return json.dumps(datasets)
