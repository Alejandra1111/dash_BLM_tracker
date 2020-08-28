import pandas as pd 
from datetime import datetime as dt

from helpers_light import city_name_add_space, city_value
import email_template 


colors = {
'background': '#23272a',
'text': '#84A7FB'
}

style_h3 = {'background': 'white', 'color': colors['text'],
            	'padding': '1.5rem', 'margin': '0rem'}

list_timespan = [{'label': 'Previous hour', 'value': 'now_1h'}, 
 {'label': 'Today', 'value': 'today'},
 {'label': 'Yesterday', 'value': 'yesterday'},
 {'label': 'Past 7 days', 'value': 'seven_days'}]


list_timespan1 = [{'label': 'Previous hour', 'value': 'now_1h'}, 
 {'label': 'Selected Day', 'value': 'today'},
 {'label': 'Previous Day', 'value': 'yesterday'},
 {'label': 'Previous 7 days', 'value': 'seven_days'}]

archive_dates0 = ['05-27', '05-30', '06-03','06-06','06-10','06-13',
			 '06-17', '06-20', '06-24', '06-27']

archive_dates = [pd.to_datetime('2020-' + d) for d in archive_dates0]


cities = ['Minneapolis','LosAngeles','Denver','Miami','Memphis',
	          'NewYork','Louisville','Columbus','Atlanta','Washington',
	          'Chicago','Boston','Oakland','StLouis','Portland',
	          'Seattle','Houston','SanFrancisco','Philadelphia','Baltimore']

cities_all = ['all_v1', 'all_v2', 'all_v3', 'all_v4', 'all_v5']


list_cities = []
for city in cities:
    list_cities.append({'label': city_name_add_space(city), 'value': city})
    
list_cities.sort(key=city_value)
list_cities.insert(0,{'label':'All Cities','value':'all'})


input_names = ['picked_city', 'picked_version', 'picked_datetime',
				'picked_hour', 'filter_keyword', 'filter_submit']

hidden_data = ['hidden_latest_datatime', 'picked_city_extended', 
			 'picked_date_extended', 'stat_sentiments', 'stat_emotions',
			 'stat_words','top_tweets', 'top_users', 'stat_type',
			 'stat_triggered_context', 'previous_email_timestamp']


current_data_cities = {}
current_city_stats =[]

## temporary timestamp 
latest_datatime = pd.to_datetime(dt(2020,7,27,16,35)).floor('h')
latest_datatime_hour = int(str(latest_datatime)[11:13])
latest_datatime_d_dt = latest_datatime.floor('d').to_pydatetime()

default_user_inputs = {
        'picked_city': 'all', 'picked_version': 1, 'picked_datetime': str(latest_datatime_d_dt),
        'picked_hour': latest_datatime_hour, 'filter_keyword': ''
    }


with open("about_text1.md", "r", encoding="utf-8") as f:
    about_text1 = f.read()

with open("about_text2.md", "r", encoding="utf-8") as f:
    about_text2 = f.read()

daily_template = email_template.daily_template


