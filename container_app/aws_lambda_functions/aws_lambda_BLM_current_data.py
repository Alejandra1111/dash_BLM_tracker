import json
from io import BytesIO
import boto3
import pandas as pd

bucket_name = 'kotasstorage1'
session = boto3.Session()
s3_client = session.client("s3")

data_path = 'app_data/'


def getData(filename):
    f = BytesIO()
    s3_client.download_fileobj(bucket_name, filename, f)
    return f.getvalue()


cities = ['Minneapolis','LosAngeles','Denver','Miami','Memphis',
	          'NewYork','Louisville','Columbus','Atlanta','Washington',
	          'Chicago','Boston','Oakland','StLouis','Portland',
	          'Seattle','Houston','SanFrancisco','Philadelphia','Baltimore']

cities_all = ['all_v1', 'all_v2', 'all_v3', 'all_v4', 'all_v5']


current_data_cities = {}
for city in (cities + cities_all):
	#print(city)
	data_path_city = data_path + 'data_current/city/' + city + '/'
	stat_sentiments = pd.read_csv(BytesIO(getData(data_path_city + 'stat_sentiments.csv')), encoding='latin-1') 
	stat_emotions = pd.read_csv(BytesIO(getData(data_path_city + 'stat_emotions.csv')), encoding='latin-1') 
	stat_words = pd.read_json(getData(data_path_city + 'stat_words.json'), orient='records', lines=True)
	top_tweets = pd.read_csv(BytesIO(getData(data_path_city + 'top_tweets.csv')), encoding='latin-1') 
	top_users = pd.read_csv(BytesIO(getData(data_path_city + 'top_users.csv')), encoding='latin-1') 
	
	current_data_cities[city] = {
		'stat_sentiments': stat_sentiments.to_json(orient='split'),
		'stat_emotions': stat_emotions.to_json(orient='split'),
		'stat_words': stat_words.to_json(orient='split'),
		'top_tweets': top_tweets.to_json(orient='split'),
		'top_users': top_users.to_json(orient='split')
	}

	
def lambda_handler(event, context):
    return json.dumps(current_data_cities)
