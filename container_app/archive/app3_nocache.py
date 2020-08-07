import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime as dt
import time
import boto3
import logging
import conf_credentials as conf
import threading
import time

from helpers_light import *

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']


client = boto3.client('lambda',
                        region_name= conf.region,
                        aws_access_key_id=conf.aws_access_key_id,
                        aws_secret_access_key=conf.aws_secret_access_key)


with open("about_text1.md", "r", encoding="utf-8") as f:
    about_text1 = f.read()

with open("about_text2.md", "r", encoding="utf-8") as f:
    about_text2 = f.read()

colors = {
'background': '#23272a',
'text': '#7289da'
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


current_data_cities = {}

## temporary timestamp 
latest_datatime = pd.to_datetime(dt(2020,7,27,16,35)).floor('h')
latest_datatime_hour = int(str(latest_datatime)[11:13])
latest_datatime_d_dt = latest_datatime.floor('d').to_pydatetime()


def load_current_data():
	global current_data_cities, latest_datatime, latest_datatime_hour, latest_datatime_d_dt 

	threading.Timer(600, load_current_data).start()
	print('Updating current data for evey 10 min:')

	result = client.invoke(FunctionName='BLM_current_data',
	                InvocationType='RequestResponse',                                      
	                Payload='{}')

	current_data_cities = json.loads(json.loads(result['Payload'].read()))
	df1 = pd.read_json(current_data_cities['all_v1']['stat_sentiments'], orient='split') 
	print('the latest data is for: ', pd.to_datetime(df1.time).max())
	latest_datatime = pd.to_datetime(df1.time).max()
	latest_datatime_hour = int(str(latest_datatime)[11:13])
	latest_datatime_d_dt = latest_datatime.floor('d').to_pydatetime()
 
load_current_data()



print('App is starting...')

app = dash.Dash(__name__, title = 'BLM Tracker', 
	external_stylesheets=external_stylesheets)

app.config['suppress_callback_exceptions'] = True
app.logger.setLevel(logging.ERROR)

def input_container_children(
	picked_city='all', picked_version=1,
	picked_datetime=str(latest_datatime_d_dt),
	picked_hour=latest_datatime_hour,
	filter_keyword='', n_clicks=0):

	return [html.Label('Select City:', 
				style={'margin': '5px 10px 5px 0'}),
			dcc.Dropdown(id='picked_city',
	    		options = list_cities,
	    		value = picked_city,
	    		clearable = False,
	    		style = {'min-width': '150px'}),
			dcc.Dropdown(id='picked_version',
	    		options = [{'label': 'Version 1', 'value': 1}],
	    		value = picked_version,
	    		clearable = False),
			html.Label('Select Date:', 
				style={'margin': '5px 10px 5px 25px'}),
			dcc.DatePickerSingle(
		        id='picked_datetime',
		        min_date_allowed=dt(2020, 6, 28),
		        max_date_allowed=latest_datatime_d_dt,
		        date=picked_datetime,
	    	),
	    	dcc.Dropdown(id='picked_hour', 
	    		options = [{'label': str(h) + ':00', 'value':h} for h in range(24)],
	    		value=picked_hour,
	    		clearable = False,
	    		style = {'min-width': '100px'},
	    		),
			dcc.Input(id='filter_keyword', type='text', value=filter_keyword,
				placeholder='Enter a keyword',
				#debounce=True,
				style = {'min-width': '80px'}
			),
			html.Button('Filter', id='filter_submit',
				n_clicks = n_clicks,
				style = {'padding': '0 20px'})
			]

input_container0 = input_container_children()
idx1 = [ item.id=='filter_keyword' if hasattr(item, 'id') else False for item in input_container0]
idx_filter_keyword = np.array(range(len(input_container0)))[idx1][0]


input_names = ['picked_city', 'picked_version', 'picked_datetime',
				'picked_hour', 'filter_keyword', 'filter_submit']


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
		html.Div(id='dynamic-input-container', 
			children=input_container_children(),
			style = {'display': 'flex', 'flex-flow': 'row wrap', 'align-items': 'center'}
			),
		html.Div(id='dynamic-note-container', 
    				children=[ html.Div(id='note_filter', children=[])]
    				),
		html.Div(id='selected_data_description'),
		dcc.Tabs(id='tabs', value='tab-sentiments', 
			children=[
	        	dcc.Tab(label='Sentiments', value='tab-sentiments'),
	        	dcc.Tab(label='Top Words', value='tab-topwords'),
	        	dcc.Tab(label='Top Tweets', value='tab-toptweets'),
	        	dcc.Tab(label='Top Users', value='tab-topusers'),
	        	dcc.Tab(label='About', value='tab-about')
    ]),
    html.Div(id='tabs-content'),
    # hidden data
    html.Div(id='picked_city_extended', style={'display': 'none'}),
    html.Div(id='picked_stats', style={'display': 'none'}, children = None), 
	],
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
    		dcc.Markdown(children=about_text1),
    		html.Div(html.Img(src='assets/program_chart.png',
    		 style={'width':'75%'})),
    		dcc.Markdown(children=about_text2)
        ])


@app.callback(
	Output('selected_data_description','children'),
	[Input('picked_stats','children')],
	[State('picked_city', 'value'),State('picked_datetime','date'),
	State('picked_hour','value'), State('filter_keyword','value'),
	State('picked_version','value')]
	)
def return_data_descripton(picked_stats, city, date, hour, filter_keyword, picked_version):
	city_str = 'All Cities' if city=='all' else city_name_add_space(city)
	if city=='all': city_str = city_str + ' (2% data sample Version <b>' + str(picked_version) + ')</b>'
	str1 = 'Selected data are for <b>' + city_str + '</b> on <b>' + str(date)[:10] + '</b>, <b>' + str(hour) + ':00 CST</b>' 
	stats = json.loads(picked_stats)
	if stats['type'] == 'filtered stats': 
		str1 = str1 + ', filtered for tweets containing word "<b>' + filter_keyword + '</b>"'
	return html.Iframe(srcDoc=str1 +'.', sandbox='',
		style={'height': '30px', 'width':'950px', 
			'margin': '0px 2px 2px 0px', 'border-radius': '5px',
    		'padding': '5px', 'color': '#23272a',
    		'background-color': '#7289DA21',
    		'border-style': 'hidden',})


@app.callback(
	Output('picked_hour','options'),
	[Input('picked_datetime', 'date')]
	)
def set_hour_to_max(date):
	max_hour = latest_datatime_hour+1 if date == str(latest_datatime_d_dt) else 24
	return [{'label': str(h) + ':00', 'value':h} for h in range(max_hour)]


@app.callback(
	Output('topwords_timespan','options'),
	[Input('picked_datetime', 'date')]
	)
def update_topwords_timespan(date):
	return list_timespan if date == str(latest_datatime_d_dt) else list_timespan1

@app.callback(
	Output('topusers_timespan','options'),
	[Input('picked_datetime', 'date')]
	)
def update_topusers_timespan(date):
	return list_timespan if date == str(latest_datatime_d_dt) else list_timespan1

@app.callback(
	Output('toptweets_timespan','options'),
	[Input('picked_datetime', 'date')]
	)
def update_topusers_timespan(date):
	return list_timespan if date == str(latest_datatime_d_dt) else list_timespan1


@app.callback(
	[Output('picked_city_extended','children'),
	Output('picked_version', 'options'),
	Output('picked_version', 'style')],
	[Input('picked_city', 'value'),
	Input('picked_version','value')]
	)
def city_extended(city, version):
	if (city=='all') & (version>0) :
		city_extended = city + '_v' + str(version) 
		return [city_extended, [{'label': 'Version '+ str(i), 'value': i}  for i in range(1,6)], {'min-width': '100px'}]
	else:
		return [city, [{'label':'none', 'value':-1}], {'display': 'none'}]


@app.callback(
	Output('note_filter','children'),
	[Input('filter_keyword','value'),
	 Input('filter_submit', 'n_clicks')]
	)
def update_note_filter(filter_keyword, n_clicks):
	if filter_keyword=='': return None
	context = dash.callback_context.triggered[0]['prop_id'].split('.')[0]
	print(context) 

	if (context=='filter_submit'):
		str1 = '<b>Please wait</b>: app is accessing <b>raw data</b>, filtering, and re-calculating statistics... This takes time.'
	else:
		str1 = 'Please note: filtering data <b>will take some time</b>. Press <b>"Filter"</b> to proceed.'
	return [html.Iframe(srcDoc=str1, sandbox='',
			style={'height': '30px', 'width':'950px', 
				'margin': '0px 2px 2px 0px', 'border-radius': '5px',
	    		'padding': '5px', 
	    		'border-style': 'hidden',})]


@app.callback(
	[Output(inputid,'disabled') for inputid in input_names],
	[Input('filter_submit','n_clicks')],
	[State('filter_keyword','value')]
	)
def disable_inputs(n_clicks, filter_keyword):
	if n_clicks is None: return [False for i in input_names]	
	if (n_clicks>0) & (filter_keyword!=''):
		print('filter_submit disabled')
		return [True for i in input_names]	
	else:
		return [False for i in input_names]	



@app.callback(
	[Output('picked_stats','children'),
	Output('dynamic-input-container', 'children'),
	Output('dynamic-note-container', 'children')],
	[Input('picked_city_extended','children'),
	Input('picked_datetime','date'),
	Input('filter_submit','n_clicks'),
	Input('picked_hour','value')],
	[State('filter_keyword','value'),
	State('picked_city','value'),
	State('picked_version','value'),
	State('dynamic-input-container', 'children'),
	State('dynamic-note-container', 'children'),
	State('picked_stats','children')
	]
	) 
def pick_stat_city_date(city, date, filter_submit, hour, 
	filter_keyword, city0, city_all_version, input_container, note_container, picked_stats):
	print('In pick_stat_city_date():')
	print('selected city date hour' + city + str(date) + str(hour) + ':00')
	
	context = dash.callback_context.triggered[0]['prop_id'].split('.')[0]
	print('triggered context:', context) 

	if picked_stats is not None:
		stats = json.loads(picked_stats)
		if (stats['type'] != 'current stats') & (context == 'picked_hour'):
			print('skipping picked_stats update')
			clear_filter_keyword(input_container, idx_filter_keyword)
			return [picked_stats, input_container, note_container]

	if (str(date) == str(latest_datatime_d_dt)) & (filter_keyword == '') & (str(hour)==str(latest_datatime_hour)):
		print('Returning current stats for ' + city)
		stats = current_data_cities[city]
		stats['type'] = 'current stats'
		clear_filter_keyword(input_container, idx_filter_keyword)
		return [json.dumps(stats), input_container, note_container]

	data = {'city': city, 'date': date[:10], 
		'filter_keyword': filter_keyword}
	

	if (context=='filter_submit') & (filter_keyword!=''):
		print('Getting stats for ' + city + ' on ' + date[:10] + ' with filter: ', filter_keyword)
		result = client.invoke(FunctionName='BLM_stats',
	                InvocationType='RequestResponse',                                      
	                Payload=json.dumps(data))

		note_container.pop()
		note_container.append(html.Div(id='note_filter', children=[]))
		input_container = input_container_children(
			picked_city=city0, picked_version=city_all_version,
			picked_datetime=date, picked_hour= hour,
			filter_keyword=filter_keyword, n_clicks=0)

	else:
		print('Getting stats for ' + city + ' on ' + date[:10])
		result = client.invoke(FunctionName='BLM_get_stats',
	                InvocationType='RequestResponse',                                      
	                Payload=json.dumps(data))
		clear_filter_keyword(input_container, idx_filter_keyword)


	print(result)
	return [json.loads(result['Payload'].read()), input_container, note_container]




'''
	callback functions to update figures and tables
'''
@app.callback(
    Output('sentiments', 'figure'),
    [Input('picked_stats','children')]
    )
def update_fig_sentiments(picked_stats):
	if picked_stats is None: return None
	print('IN update_fig_sentiments():')
	stats = json.loads(picked_stats)
	df = pd.read_json(stats['stat_sentiments'], orient='split',
		convert_dates=['time'], date_unit='ms')
	# print(df)
	return fig_sentiments(df)

@app.callback(
    Output('emotions', 'figure'),
    [Input('picked_stats','children')]
    )
def update_fig_emotions(picked_stats):
	if picked_stats is None: return None
	print('IN update_fig_emotions():')
	stats = json.loads(picked_stats)
	df = pd.read_json(stats['stat_emotions'], orient='split',
		convert_dates=['time'], date_unit='ms'
		)
	return fig_emotions(df)


@app.callback(
    Output('word_bars', 'figure'),
    [Input('picked_stats','children'),
    Input('topwords_timespan','value'),
    Input('picked_hour','value')]
    )
def update_fig_word_bars(picked_stats, timespan, hour):
	if picked_stats is None: return None
	print('IN update_fig_word_bars():')
	stats = json.loads(picked_stats)
	df = pd.read_json(stats['stat_words'], orient='split')
	print(df)
	print(stats['type'])
	timespan2 = get_timespan(hour, timespan)
	return fig_word_bars(df, subset=timespan2)


@app.callback(
     Output('plt_word_cloud', 'src'),
    [Input('picked_stats','children'),
    Input('topwords_timespan','value'),
    Input('picked_hour','value')]
    )
def update_word_cloud(picked_stats, timespan, hour):
	if picked_stats is None: return None
	print('IN update_word_cloud():')
	stats = json.loads(picked_stats)
	df = pd.read_json(stats['stat_words'], orient='split')
	timespan2 = get_timespan(hour, timespan)
	fig, ax1 = plt.subplots(1,1)
	fig = fig_word_cloud(df, subset=timespan2)
	out_url = fig_to_uri(fig)
	return out_url


@app.callback(
    Output('users_bars', 'figure'),
    [Input('picked_stats','children'),
    Input('topusers_timespan','value'),
    Input('picked_hour','value')]
    )
def update_fig_users_bars(picked_stats, timespan, hour):
	if picked_stats is None: return None
	print('IN update_fig_users_bars():')
	stats = json.loads(picked_stats)
	df = pd.read_json(stats['top_users'], orient='split')
	fix_user_id(df)
	#print(df)
	timespan2 = get_timespan(hour, timespan)
	return fig_top_users(df, subset=timespan2)


@app.callback(
    Output('pass_tbl_top_tweets', 'children'),
    [Input('picked_stats','children'),
    Input('toptweets_timespan','value'),
    Input('picked_hour','value')]
    )
def update_tweet_table(picked_stats, timespan, hour):
	if picked_stats is None: return None
	print('IN update_tweet_table():')
	stats = json.loads(picked_stats)
	df = pd.read_json(stats['top_tweets'], orient='split')
	fix_RT_id(df)
	#print(df)
	timespan2 = get_timespan(hour, timespan)
	return gen_dash_table('tbl_top_tweets', df, 
		subset=timespan2, type='top_tweets')

@app.callback(
    Output('pass_tbl_top_users', 'children'),
    [Input('picked_stats','children'),
    Input('topusers_timespan','value'),
    Input('picked_hour','value')]
    )
def update_user_table(picked_stats, timespan, hour):
	if picked_stats is None: return None
	print('IN update_user_table():')
	stats = json.loads(picked_stats)
	df = pd.read_json(stats['top_users'], orient='split')
	fix_user_id(df)
	#print(df)
	timespan2 = get_timespan(hour, timespan)
	return gen_dash_table('tbl_top_users', df, 
		subset=timespan2, type='top_users')


if __name__ == '__main__':
	app.run_server(host='0.0.0.0', port=8050, debug=True) #, dev_tools_ui=False) #, dev_tools_props_check=False)

