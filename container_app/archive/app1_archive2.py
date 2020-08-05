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


from helpers import *

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

data_path = '/Users/kotaminegishi/big_data_training/python/dash_demo1/'

# load current data
stat_sentiments = pd.read_csv(data_path + 'data_current/stat_sentiments.csv')
stat_emotions = pd.read_csv(data_path + 'data_current/stat_emotions.csv')
stat_words = pd.read_json(data_path + 'data_current/stat_words.json', orient='records', lines=True)
top_tweets = pd.read_csv(data_path + 'data_current/top_tweets.csv')
top_users = pd.read_csv(data_path + 'data_current/top_users.csv')

fix_datetime(stat_sentiments, 'time')
fix_datetime(stat_emotions, 'time')
fix_token_counter(stat_words)

## overwrite this for testing
#latest_datatime = stat_sentiments.time.max()
latest_datatime = pd.to_datetime(dt(2020,7,13,4,35)).floor('h')
latest_datatime_hour = int(str(latest_datatime)[11:13])
latest_datatime_d_dt = latest_datatime.floor('d').to_pydatetime()

# load cumulative data
#cum_original, cum_retweet, cum_words = load_cum_data(latest_datatime)

with open("about_text.md", "r", encoding="utf-8") as f:
    about_text = f.read()



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
cache.clear() # uncomment this to clear all cache


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

list_cities = []
for city in cities:
    list_cities.append({'label': city_name_add_space(city), 'value': city})
    
list_cities.sort(key=city_value)
list_cities.insert(0,{'label':'--','value':'all'})


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
		dcc.DatePickerSingle(
	        id='picked_datetime',
	        min_date_allowed=dt(2020, 6, 28),
	        max_date_allowed=latest_datatime_d_dt,
	        initial_visible_month=latest_datatime_d_dt,
	        date=str(latest_datatime_d_dt)
    	),
    	dcc.Dropdown(id='picked_hour', 
    		options = [{'label': str(h) + ':00', 'value':h} for h in range(24)],
    		value=latest_datatime_hour
    		),
    	dcc.Dropdown(id='picked_city',
    		options = list_cities,
    		value = 'all'
    		),
    	div(id='cities_all_version'),
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
    # hidden signal
    html.Div(id='signal', style={'display': 'none'}),     
    # hidden filtered datasets
    html.Div(id='filtered_datasets', style={'display': 'none'}),
	],
	#style={'backgroundColor': colors['background'],
	style={'maxWidth': '960px', 'margin': 'auto'},
)


@app.callback(Output('tabs-content', 'children'),
              [Input('tabs', 'value')])
def render_content(tab):
    if tab == 'tab-sentiments': 
    	return html.Div([
			dcc.Graph(
				figure = fig_sentiments(stat_sentiments)
			),
			dcc.Graph(
				figure = fig_emotions(stat_emotions)
			)
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
		    #html.Div(id='filtered_stat_words', style={'display': 'none'}),
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
            html.Div(id='pass_tbl_top_tweets')
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
            html.Div(id='pass_tbl_top_users') 
        ])
    elif tab == 'tab-about': 
    	return  html.Div([
    		dcc.Markdown(children=about_text)
        ])


# not sure if this is allowed
@app.callback(
    Output('cities_all_version', 'children'),
    [Input('picked_city', 'value')])
def set_cities_options(selected_city):
	if selected_city!='all':
		return None
    return dcc.Dropdown(
    	id = 'picked_cities_all_version',
    	options = [{'label': '10% sample version ' + str(1+i), 'value': 'all_v' + str(1+i)} for i in range(5)],
		value = 'all_v1'
		)

'''
@app.callback(
    Output('cities-radio', 'value'),
    [Input('cities-radio', 'options')])
def set_cities_value(available_options):
    return available_options[0]['value']
'''




@cache.memoize()
def load_cum_data(base_timestamp):
	print('In load_cum_data():')
	print('base_timestamp:', base_timestamp)
	
	print('Loading cumulative data: sentiments and emotions...')
	files_sentiments = keep_recent_files(glob(data_path + "data_cumulative_s/sentiments/*"),
                            base_timestamp=base_timestamp,
                            file_type = '.csv', days=14) 
	cum_sentiments = tw_data_files_to_df_csv2(files_sentiments, frac=0.5, float_dtype='float16')
	cum_sentiments = cum_sentiments.drop_duplicates(subset = 'id')

	files_emotions = keep_recent_files(glob(data_path + "data_cumulative_s/emotions/*"),
	                    base_timestamp=base_timestamp,
	                    file_type = '.csv', days=14)
	cum_emotions = tw_data_files_to_df_csv2(files_emotions, frac=0.5, float_dtype='float16')
	cum_emotions = cum_emotions.drop_duplicates(subset = 'id')


	print('Loading cumulative data: words...')
	files_words = keep_recent_files(glob(data_path + "data_cumulative_s/words/*"),
	                                base_timestamp=base_timestamp,
	                                file_type = '.json', days=7) 
	cum_words = tw_data_files_to_df_json3(files_words, lines=True, frac=0.5, float_dtype='float16')

	print('Loading cumulative data: original and retweet...')
	# load recent cumulative data     
	files_original = keep_recent_files(glob(data_path + "data_cumulative_s/original/*"),
	    base_timestamp = base_timestamp, days=7)
	cum_original = tw_data_files_to_df_json3(files_original, lines=True, frac=0.5, float_dtype='float16')


	files_retweet = keep_recent_files(glob(data_path + "data_cumulative_s/retweet/*"),
	    base_timestamp = base_timestamp, days=365)
	cum_retweet = tw_data_files_to_df_json3(files_retweet, lines=True, float_dtype='float16')
	

	datasets = {
         'cum_original': cum_original.to_json(orient='split'),
         'cum_retweet': cum_retweet.to_json(orient='split'),
         'cum_words': cum_words.to_json(orient='split'),
         'cum_sentiments': cum_sentiments.to_json(orient='split'),
         'cum_emotions': cum_emotions.to_json(orient='split')
     }
	print('Exiting load_cum_data():')

	return json.dumps(datasets)


@app.callback(Output('signal', 'children'),
	[Input('picked_datetime','date')]
	)
def call_load_cum_data(date):
	base_time = pd.to_datetime(date)
	print('IN store_current_cum_data(): ', base_time)
	# load_cum_data(base_time) # for a shortcut in developing stage
	return date 


@app.callback(
	Output('filtered_datasets','children'),
	[Input('signal', 'children'),
	Input('filter_keyword','value')]
	)
def filter_process(date, filter_word):
	# add filter process 
	print('In filter_process():')
	base_time =  pd.to_datetime(date)
	#if filter_word == '':
	if True: # for a shortcut in developing stage 
		stat_sentiments2, stat_emotions2, stat_words2, top_tweets2, top_users2 = stat_sentiments, stat_emotions, stat_words, top_tweets, top_users	
	else:
		print('loading cumulative datasets..')
		# load cumulative data
		datasets = json.loads(load_cum_data(base_time))
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

		print(cum_original)
		stat_sentiments2, stat_emotions2, stat_words2, top_tweets2, top_users2 = filter_data(
			filter_word, cum_original, cum_retweet, cum_words, cum_sentiments, cum_emotions, time=base_time)

	datasets = {
         'stat_sentiments': stat_sentiments2.to_json(orient='split'),
         'stat_emotions': stat_emotions2.to_json(orient='split'),
         'stat_words': stat_words2.to_json(orient='split'),
         'top_tweets': top_tweets2.to_json(orient='split'),
         'top_users': top_users2.to_json(orient='split')
    }
	print(stat_words2)

	return json.dumps(datasets)


@app.callback(
    Output('word_bars', 'figure'),
    [Input('filtered_datasets','children'),
    Input('topwords_timespan','value')]
    )
def update_fig_word_bars(filtered_datasets, timespan):
	print('IN update_fig_word_bars():')
	datasets = json.loads(filtered_datasets)
	df = pd.read_json(datasets['stat_words'], orient='split')
	print(df)
	return fig_word_bars(df, subset=timespan)


@app.callback(
     Output('plt_word_cloud', 'src'),
    [Input('filtered_datasets','children'),
    Input('topwords_timespan','value')]
    )
def update_word_cloud(filtered_datasets, timespan):
	print('IN update_word_cloud():')
	datasets = json.loads(filtered_datasets)
	stat_words = pd.read_json(datasets['stat_words'], orient='split')
	print(stat_words)

	fig, ax1 = plt.subplots(1,1)
	fig = fig_word_cloud(stat_words, subset='now_1h')
	out_url = fig_to_uri(fig)
	return out_url


@app.callback(
    Output('users_bars', 'figure'),
    [Input('filtered_datasets','children'),
    Input('topusers_timespan','value')]
    )
def update_fig_users_bars(filtered_datasets, timespan):
	print('IN update_fig_users_bars():')
	datasets = json.loads(filtered_datasets)
	df = pd.read_json(datasets['top_users'], orient='split')
	print(df)
	return fig_top_users(df, subset=timespan)


@app.callback(
    Output('pass_tbl_top_tweets', 'children'),
    [Input('filtered_datasets','children'),
    Input('toptweets_timespan','value')]
    )
def update_tweet_table(filtered_datasets, timespan):
	print('IN update_tweet_table():')
	datasets = json.loads(filtered_datasets)
	df = pd.read_json(datasets['top_tweets'], orient='split')
	print(df)
	return gen_dash_table('tbl_top_tweets', df, 
		subset=timespan, type='top_tweets')

@app.callback(
    Output('pass_tbl_top_users', 'children'),
    [Input('filtered_datasets','children'),
    Input('topusers_timespan','value')]
    )
def update_tweet_table(filtered_datasets, timespan):
	print('IN update_tweet_table():')
	datasets = json.loads(filtered_datasets)
	df = pd.read_json(datasets['top_users'], orient='split')
	print(df)
	return gen_dash_table('tbl_top_users', df, 
		subset=timespan, type='top_users')


if __name__ == '__main__':
	app.run_server(debug=True) #, dev_tools_ui=False) #, dev_tools_props_check=False)

