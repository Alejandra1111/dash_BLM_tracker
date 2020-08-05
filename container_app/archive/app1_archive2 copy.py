import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import pandas as pd
import json
import os
from datetime import datetime as dt
import copy
from flask_caching import Cache
from glob import glob
import random 

from helpers import *

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

data_path = '/Users/kotaminegishi/big_data_training/python/dash_demo1/'


## overwrite this for testing
#latest_datatime = stat_sentiments.time.max()
latest_datatime = pd.to_datetime(dt(2020,7,19,20,35)).floor('h')
latest_datatime_hour = int(str(latest_datatime)[11:13])
latest_datatime_d_dt = latest_datatime.floor('d').to_pydatetime()

with open("about_text.md", "r", encoding="utf-8") as f:
    about_text = f.read()

colors = {
'background': '#23272a',
'text': '#7289da'
}

style_h3 = {'background': 'white', 'color': colors['text'],
            	'padding': '1.5rem', 'margin': '0rem'}

list_timespan = [{'label': 'Last hour', 'value': 'now_1h'}, 
 {'label': 'Today', 'value': 'today'},
 {'label': 'Yesterday', 'value': 'yesterday'},
 {'label': 'Past 7 days', 'value': 'seven_days'}]

cities = ['Minneapolis','LosAngeles','Denver','Miami','Memphis',
	          'NewYork','Louisville','Columbus','Atlanta','Washington',
	          'Chicago','Boston','Oakland','StLouis','Portland',
	          'Seattle','Houston','SanFrancisco','Philadelphia','Baltimore']

cities_all = ['all_v1', 'all_v2', 'all_v3', 'all_v4', 'all_v5']

list_cities = []
for city in cities:
    list_cities.append({'label': city_name_add_space(city), 'value': city})
    
list_cities.sort(key=city_value)
list_cities.insert(0,{'label':'--','value':'all'})


# load current data
current_data_cities = {}
for city in (cities + cities_all):
	data_path_city = data_path + 'data_current/city/' + city + '/'
	stat_sentiments = pd.read_csv(data_path_city + 'stat_sentiments.csv')
	stat_emotions = pd.read_csv(data_path_city + 'stat_emotions.csv')
	stat_words = pd.read_json(data_path_city + 'stat_words.json', orient='records', lines=True)
	top_tweets = pd.read_csv(data_path_city + 'top_tweets.csv')
	top_users = pd.read_csv(data_path_city + 'top_users.csv')
	
	current_data_cities[city] = {
		'stat_sentiments': stat_sentiments.to_json(orient='split'),
		'stat_emotions': stat_emotions.to_json(orient='split'),
		'stat_words': stat_words.to_json(orient='split'),
		'top_tweets': top_tweets.to_json(orient='split'),
		'top_users': top_users.to_json(orient='split')
	}

hour_for_caching = '23:59'

null_cum_sentiments, null_cum_emotions, null_cum_words, null_cum_original, null_cum_retweet = load_null_df(data_path)


print('App is starting...')

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.config['suppress_callback_exceptions'] = True

CACHE_CONFIG = {
    # try 'filesystem' if you don't want to setup redis
    'CACHE_TYPE': 'redis',
    'CACHE_REDIS_URL': os.environ.get('REDIS_URL', 'redis://localhost:6379')
}
#cache = Cache(config=CACHE_CONFIG)
#cache.init_app(app.server)
cache = Cache()
cache.init_app(app.server, config=CACHE_CONFIG)
#cache.clear() # uncomment this to clear all cache


app.layout = html.Div(
	children = [
	    html.H1(
			"Tweets for #BlackLivesMatter",
			style = {'textAlign': 'center', 'color': colors['text'],
			'backgroundColor': colors['background'], 
			'padding-top': '1.5rem','padding-bottom': '0.5rem', 'margin': '0rem'}
		),
		html.H5(			
			"Track the BLM movement with Twitter Data. ",
			style = {'textAlign': 'center', 'color': colors['text'],
			'backgroundColor': colors['background'],
			'padding-bottom': '0.5rem', 'margin': '0rem'}
		),
		dcc.Dropdown(id='picked_city',
    		options = list_cities,
    		value = 'all',
    		clearable = False
    		#placeholder = 'Select a city: "--" represents a sample of all data.'
    		),
		dcc.DatePickerSingle(
	        id='picked_datetime',
	        min_date_allowed=dt(2020, 6, 28),
	        max_date_allowed=latest_datatime_d_dt,
	        initial_visible_month=latest_datatime_d_dt,
	        date=str(latest_datatime_d_dt),
    	),
    	dcc.Dropdown(id='picked_hour', 
    		options = [{'label': str(h) + ':00', 'value':h} for h in range(24)],
    		value=latest_datatime_hour,
    		clearable = False
    		),
		dcc.Input(id='filter_keyword', type='text', value='',
		    	placeholder='Type a keyword to filter data',
		    	debounce=True),
		dcc.Tabs(id='tabs', value='tab-sentiments', 
			children=[
	        	dcc.Tab(label='Sentiments', value='tab-sentiments'),
	        	dcc.Tab(label='Top Words', value='tab-topwords'),
	        	dcc.Tab(label='Top Tweets', value='tab-toptweets'),
	        	dcc.Tab(label='Top Users', value='tab-topusers'),
	        	dcc.Tab(label='About', value='tab-about')
    ]),
    html.Div(id='tabs-content'),
    # hidden extended city name
    html.Div(id='picked_city_extended', style={'display': 'none'}),
    # hidden signal
    html.Div(id='signal', style={'display': 'none'}),
    html.Div(id='signal2', style={'display': 'none'}),          
    # hidden stats
    html.Div(id='picked_stats', style={'display': 'none'},
    	children = json.dumps(current_data_cities['all_v1'])),          
	],
	#style={'backgroundColor': colors['background'],
	style={'maxWidth': '960px', 'margin': 'auto'},
)


@app.callback(Output('tabs-content', 'children'),
              [Input('tabs', 'value')])
def render_content(tab):
    if tab == 'tab-sentiments': 
    	return html.Div([
			dcc.Graph(id = 'sentiments'),
			dcc.Graph(id = 'emotions')
			])
    elif tab == 'tab-topwords': 
    	return html.Div([
            html.H3('Top words', style = style_h3),
		    dcc.RadioItems(
		    	id = 'topwords_timespan',
		        options=list_timespan,
		        value='now_1h',
		        labelStyle={"display": "inline-block"},
		    ),
		    html.Br(),
           	html.Div([html.Img(id = 'plt_word_cloud', src = '',
           		style = {'position': 'relative', 'top': '-180px',})],
             id='word_cloud',
             style = {'height': '640px','overflow': 'hidden',}),
            dcc.Graph(id = 'word_bars')
        ])
    elif tab == 'tab-toptweets': 
    	return html.Div([
            html.H3('Top tweets', style = style_h3),
            dcc.RadioItems(
		    	id = 'toptweets_timespan',
		        options=list_timespan,
		        value='now_1h',
		        labelStyle={"display": "inline-block"},
		    ),
		    html.Br(),
            html.Div(id='pass_tbl_top_tweets',
            	 style = {'margin-bottom': '300px'})
        ])
    elif tab == 'tab-topusers': 
    	return html.Div([
            html.H3('Top users', style = style_h3),
            dcc.RadioItems(
		    	id = 'topusers_timespan',
		        options=list_timespan,
		        value='now_1h',
		        labelStyle={"display": "inline-block"},
		    ),
            dcc.Graph(id = 'users_bars'),
            html.Div(id='pass_tbl_top_users',
            	style = {'margin-bottom': '300px'}) 
        ])
    elif tab == 'tab-about': 
    	return  html.Div([
    		dcc.Markdown(
    			'''
    			## About this app

    			This site is a web application of **Twitter** data gathering, 
    			storage, and analysis on hashtag **BlackLivesMatter**. 
    			It is powered by **Twitter API**, **AWS Kinesis**, **Python**, 
    			and **Dash**. [Source code](link_here).

    			### Data Collection and Processing
    			'''
    			),
    		html.H4('Data Flow Chart'),
    		html.Div(html.Img(src='assets/program_chart.png',
    		 style={'width':'75%'})),
    		dcc.Markdown(children=about_text)
        ])



@app.callback(
	Output('picked_city_extended','children'),
	[Input('picked_city', 'value')]
	)
def city_extended(city):
	if city=='all':
		# randomly pick one of five versions of all_v1 etc. 
		return city + '_v' + str(random.choice([1,2,3,4,5]))
	else:
		default_city = False 
		return city


@app.callback(Output('signal', 'children'),
	[Input('picked_datetime','date')],
	[State('picked_city_extended','children')]
	)
def call_load_city_date_from_data_change(date, city):
	print('IN call_load_city_date_from_data_change(): ', date, ': ', city)
	date_cache = pd.to_datetime(date[:10] + ' ' + hour_for_caching)
	load_city_date_data(city_date(city, date_cache))
	return date 

@app.callback(Output('signal2', 'children'),
	[Input('picked_city_extended','children')],
	[State('picked_datetime','date')]
	)
def call_load_city_date_from_city_change(city, date):
	print('IN call_load_city_date_from_city_change(): ', date, ': ', city)
	# skip loading city_date data if date == str(latest_datatime_d_dt)
	if date != str(latest_datatime_d_dt):
		date_cache = pd.to_datetime(date[:10] + ' ' + hour_for_caching)

		load_city_date_data(city_date(city, date_cache))
	return city


@app.callback(
	Output('picked_stats','children'),
	[Input('signal', 'children'),
	Input('signal2', 'children'),
	Input('filter_keyword','value'),
	Input('picked_hour','value')],
	[State('picked_city_extended','children'),
	State('picked_datetime','date')]
	)
def pick_datasets(signal, signal2, filter_word, hour, city, date):
	hour = '0' + str(hour) if len(str(hour))==1 else str(hour)
	picked_time = pd.to_datetime(date[:10] + ' ' + hour)
	date_cache = pd.to_datetime(date[:10] + ' ' + hour_for_caching)
	print('In pick_datasets():',  picked_time)

	if (date == str(latest_datatime_d_dt)) & (filter_word == '') & (hour==str(latest_datatime_hour)):
		# use current data
		print('Returning current datasets for ' + city)
		stats = current_data_cities[city]
		#print(stats['stat_sentiments'])
		stats['time'] = str(picked_time)
		return json.dumps(stats)

	print('loading cumulative datasets..')
	# load cumulative data
	datasets = json.loads(load_city_date_data(city_date(city, date_cache)))
	cum_original = pd.read_json(datasets['cum_original'], orient='split')
	cum_retweet = pd.read_json(datasets['cum_retweet'], orient='split')
	cum_words = pd.read_json(datasets['cum_words'], orient='split')
	cum_sentiments = pd.read_json(datasets['cum_sentiments'], orient='split')
	cum_emotions = pd.read_json(datasets['cum_emotions'], orient='split')

	# correct data types
	fix_datetime(cum_sentiments, timevar='created_at_h')
	fix_datetime(cum_emotions, timevar='created_at_h')
	fix_datetime(cum_words, timevar='created_at_h')
	fix_token_counter(cum_words)
	fix_datetime(cum_original, timevar='created_at_h')
	fix_datetime(cum_retweet, timevar='created_at_h')
	fix_RT_id(cum_original)
	fix_RT_id(cum_retweet)

	#print(cum_original)
	if filter_word != '':
		print('filtering data..')
		stat_sentiments, stat_emotions, stat_words, top_tweets, top_users = filter_data(
			filter_word, cum_original, cum_retweet, cum_words, cum_sentiments, cum_emotions, time=picked_time)
	else:
		stat_sentiments, stat_emotions, stat_words, top_tweets, top_users = get_stats(
			cum_original, cum_retweet, cum_words, cum_sentiments, cum_emotions, time=picked_time)

	stats = {
         'stat_sentiments': stat_sentiments.to_json(orient='split'),
         'stat_emotions': stat_emotions.to_json(orient='split'),
         'stat_words': stat_words.to_json(orient='split'),
         'top_tweets': top_tweets.to_json(orient='split'),
         'top_users': top_users.to_json(orient='split'),
         'time': str(picked_time)
    }
	#print('stat_words', stat_words)

	return json.dumps(stats)



'''
	caching function
'''
@cache.memoize()
def load_city_date_data(city_datetime):
	print('In load_city_date_data(%s):' % city_datetime)
	city, base_timestamp = city_datetime.split('#') 
	base_timestamp = pd.to_datetime(base_timestamp)
	if city is None: return None

	# load recent cumulative data
	print('  Loading cumulative data: sentiments and emotions...')
	print(data_path + "data_cumulative/city_date/" + city)
	cum_data_path = data_path + "data_cumulative/city_date/" + city
	curr_data_path = data_path + "data_current/city/" + city

	files_sentiments = keep_recent_files(glob(cum_data_path + "/sentiments/*"),
	                    base_timestamp=base_timestamp, prefix='records_',
	                    file_type = '.csv', days=15, no_newer=True) 
	#print(files_sentiments)
	if len(files_sentiments)>0: 
	    cum_sentiments = tw_data_files_to_df_csv(files_sentiments)
	    cum_sentiments = cum_sentiments.drop_duplicates(subset = 'id')
	    #fix_datetime(cum_sentiments)
	else:
	    cum_sentiments = null_cum_sentiments
	    
	files_emotions = keep_recent_files(glob(cum_data_path + "/emotions/*"),
	                    base_timestamp=base_timestamp, prefix='records_',
	                    file_type = '.csv', days=15, no_newer=True) 
	if len(files_emotions)>0:
	    cum_emotions = tw_data_files_to_df_csv(files_emotions)
	    cum_emotions = cum_emotions.drop_duplicates(subset = 'id')
	    #fix_datetime(cum_emotions)    
	else:
	    cum_emotions = null_cum_emotions

	print('  Loading cumulative data: words...')
	files_words = keep_recent_files(glob(cum_data_path + "/words/*"),
	                                base_timestamp=base_timestamp, prefix='records_',
	                                file_type = '.json', days=8, no_newer=True) 

	if len(files_words)>0:
	    cum_words = tw_data_files_to_df_json(files_words, lines=True)
	    #fix_datetime(cum_words)
	    #fix_token_counter(cum_words)
	else:
	    cum_words = null_cum_words

	print('  Loading cumulative data: original tweets and retweets...')   
	# load recent cumulative data     
	files_original = keep_recent_files(glob(cum_data_path + "/original/*"),
	    base_timestamp = base_timestamp, prefix='records_', 
	    file_type = '.json', days=8, no_newer=True)
	if len(files_original)>0:
	    cum_original = tw_data_files_to_df_json(files_original, lines=True)
	    #fix_datetime(cum_original)        
	    #fix_RT_id(cum_original)
	else:
	    cum_original = null_cum_original

	files_retweet = cum_data_path + "/retweet/2020_all_retweets.json"
	try: 
	    cum_retweet = pd.read_json(files_retweet, orient='records', lines=True)
	    #fix_datetime(cum_retweet)
	    #fix_RT_id(cum_retweet)
	except:
	    cum_retweet = null_cum_retweet


	datasets = {
	     'cum_original': cum_original.to_json(orient='split'),
	     'cum_retweet': cum_retweet.to_json(orient='split'),
	     'cum_words': cum_words.to_json(orient='split'),
	     'cum_sentiments': cum_sentiments.to_json(orient='split'),
	     'cum_emotions': cum_emotions.to_json(orient='split')
	 }
	print('Exiting load_city_date_data():')

	return json.dumps(datasets)


'''
	callback functions to update figures and tables
'''
@app.callback(
    Output('sentiments', 'figure'),
    [Input('picked_stats','children')]
    )
def update_fig_sentiments(picked_stats):
	print('IN update_fig_sentiments():')
	stats = json.loads(picked_stats)
	df = pd.read_json(stats['stat_sentiments'], orient='split',
		convert_dates=['time'], date_unit='ms')
	#fix_datetime(df, timevar='time')
	#print(df)
	return fig_sentiments(df)

@app.callback(
    Output('emotions', 'figure'),
    [Input('picked_stats','children')]
    )
def update_fig_emotions(picked_stats):
	print('IN update_fig_emotions():')
	stats = json.loads(picked_stats)
	df = pd.read_json(stats['stat_emotions'], orient='split',
		convert_dates=['time'], date_unit='ms')
	#fix_datetime(df, timevar='time')
	#print(df)
	return fig_emotions(df)


@app.callback(
    Output('word_bars', 'figure'),
    [Input('picked_stats','children'),
    Input('topwords_timespan','value')]
    )
def update_fig_word_bars(picked_stats, timespan):
	print('IN update_fig_word_bars():')
	stats = json.loads(picked_stats)
	df = pd.read_json(stats['stat_words'], orient='split')
	#print(df)
	return fig_word_bars(df, subset=timespan)


@app.callback(
     Output('plt_word_cloud', 'src'),
    [Input('picked_stats','children'),
    Input('topwords_timespan','value')]
    )
def update_word_cloud(picked_stats, timespan):
	print('IN update_word_cloud():')
	stats = json.loads(picked_stats)
	stat_words = pd.read_json(stats['stat_words'], orient='split')
	#print(stat_words)

	fig, ax1 = plt.subplots(1,1)
	fig = fig_word_cloud(stat_words, subset=timespan)
	out_url = fig_to_uri(fig)
	return out_url


@app.callback(
    Output('users_bars', 'figure'),
    [Input('picked_stats','children'),
    Input('topusers_timespan','value')]
    )
def update_fig_users_bars(picked_stats, timespan):
	print('IN update_fig_users_bars():')
	stats = json.loads(picked_stats)
	df = pd.read_json(stats['top_users'], orient='split')
	fix_user_id(df)
	#print(df)
	return fig_top_users(df, subset=timespan)


@app.callback(
    Output('pass_tbl_top_tweets', 'children'),
    [Input('picked_stats','children'),
    Input('toptweets_timespan','value')]
    )
def update_tweet_table(picked_stats, timespan):
	print('IN update_tweet_table():')
	stats = json.loads(picked_stats)
	df = pd.read_json(stats['top_tweets'], orient='split')
	fix_RT_id(df)
	#print(df)
	return gen_dash_table('tbl_top_tweets', df, 
		subset=timespan, type='top_tweets')

@app.callback(
    Output('pass_tbl_top_users', 'children'),
    [Input('picked_stats','children'),
    Input('topusers_timespan','value')]
    )
def update_user_table(picked_stats, timespan):
	print('IN update_user_table():')
	stats = json.loads(picked_stats)
	df = pd.read_json(stats['top_users'], orient='split')
	#print(df)
	return gen_dash_table('tbl_top_users', df, 
		subset=timespan, type='top_users')


if __name__ == '__main__':
	app.run_server(debug=True) #, dev_tools_ui=False) #, dev_tools_props_check=False)

