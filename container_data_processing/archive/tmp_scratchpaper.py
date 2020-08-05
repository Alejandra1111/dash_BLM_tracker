
city = 'Minneapolis'

city = 'all_v1'


city_date_stats_path = data_path + "data_cumulative/city_date/" + city + "/stats"
data_path_city = data_path + 'data_current/city/Minneapolis/'

stat_sentiments = pd.read_csv(data_path_city + 'stat_sentiments.csv')
stat_emotions = pd.read_csv(data_path_city + 'stat_emotions.csv')
stat_words = pd.read_json(data_path_city + 'stat_words.json', orient='records', lines=True)
top_tweets = pd.read_csv(data_path_city + 'top_tweets.csv')
top_users = pd.read_csv(data_path_city + 'top_users.csv')


base_timestamp = pd.to_datetime(datetime(2020,7, d, 23, 59)).floor('h')
base_timestamp

filename = city_date_stats_path + '/stats_'+ str(datetime).replace(" ",'_') + '.json'


stats = {
 'stat_sentiments': stat_sentiments.to_json(orient='split'),
 'stat_emotions': stat_emotions.to_json(orient='split'),
 'stat_words': stat_words.to_json(orient='split'),
 'top_tweets': top_tweets.to_json(orient='split'),
 'top_users': top_users.to_json(orient='split'),
 'timestamp': str(base_timestamp)
}


filename = city_date_stats_path + '/stats_'+ str(base_timestamp).replace(" ",'_') + '.json'
new_file = glob(filename)==[]

import json

with open(filename, 'w') as file:
    json.dump(stats, file)





with open(filename) as file:
    stats2 = json.load(file)


stat_words2 = pd.read_json(stats2['stat_words'], orient='split')
top_tweets2 = pd.read_json(stats2['top_tweets'], orient='split')
top_users2 = pd.read_json(stats2['top_users'], orient='split')

stat_words2[stat_words2.subset.apply(lambda x: x.startswith('now_'))]


filename =  glob(city_date_stats_path + "/*")[0]

with open(filename) as file:
    stats3 = json.load(file)

stat_words3 = pd.read_json(stats3['stat_words'], orient='split')





