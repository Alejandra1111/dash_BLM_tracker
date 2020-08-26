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
import sqlite3
import yagmail
import chart_studio
import chart_studio.plotly as py

import conf_credentials as conf
import email_template 

from helpers_light import *

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
external_scripts = [
   {'src': 'https://platform-api.sharethis.com/js/sharethis.js#property=5f3ef9be12340c0012eef1bc&product=inline-share-buttons',
    'async':'async'
    }
]

db = "blm_daily.db"
pd.set_option('mode.chained_assignment',None)

chart_studio.tools.set_credentials_file(
    username= conf.chart_studio['username'], 
    api_key= conf.chart_studio['api_key'])


client = boto3.client('lambda',
                        region_name= conf.region,
                        aws_access_key_id=conf.aws_access_key_id,
                        aws_secret_access_key=conf.aws_secret_access_key)


yag = yagmail.SMTP(
	user=conf.yag['user'],
	password=conf.yag['password'])


with open("about_text1.md", "r", encoding="utf-8") as f:
    about_text1 = f.read()

with open("about_text2.md", "r", encoding="utf-8") as f:
    about_text2 = f.read()

daily_template = email_template.daily_template

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


current_data_cities = {}
current_city_stats =[]

## temporary timestamp 
latest_datatime = pd.to_datetime(dt(2020,7,27,16,35)).floor('h')
latest_datatime_hour = int(str(latest_datatime)[11:13])
latest_datatime_d_dt = latest_datatime.floor('d').to_pydatetime()



print('App is starting...')

app = dash.Dash(__name__, title = 'BLM Tracker', 
	external_stylesheets=external_stylesheets,
	external_scripts=external_scripts)

app.config['suppress_callback_exceptions'] = True
app.logger.setLevel(logging.ERROR)
server = app.server


def input_container_children(
	picked_city='all', picked_version=1,
	picked_datetime=str(latest_datatime_d_dt),
	picked_hour=latest_datatime_hour,
	filter_keyword='', n_clicks=0,
	task_in_progress=False):
	return [html.Label('Select City:', 
				style={'margin': '5px 10px 5px 0'}),
			dcc.Dropdown(id='picked_city',
				options = list_cities,
				value = picked_city,
				clearable = False,
				style = {'minWidth': '150px'}),
			dcc.Dropdown(id='picked_version',
				options = [{'label': 'Version ' + str(i+1), 'value': i+1}
				 for i, ver in enumerate(cities_all)],
				value = picked_version,
				style = {'minWidth': '100px'},
				clearable = False),
			html.Label('Select Date:', 
				style={'margin': '5px 10px 5px 25px'}),
			dcc.DatePickerSingle(
		        id='picked_datetime',
		        min_date_allowed=dt(2020, 5, 27),
		        max_date_allowed=str(latest_datatime_d_dt),
		        date=picked_datetime,
			),
			dcc.Dropdown(id='picked_hour', 
				options = [{'label': str(h) + ':00', 'value':h} for h in range(24)],
				value=picked_hour,
				clearable = False,
				style = {'minWidth': '100px'},
				),
			dcc.Input(id='filter_keyword', type='text', value=filter_keyword,
				placeholder='Enter a keyword',
				#debounce=True,
				style = {'minWidth': '80px'}
			),
			html.Button('Filter', id='filter_submit',
				n_clicks = n_clicks,
				style = {'padding': '0 20px'}),
			# hidden data
			html.Div(id= 'task_in_progress', 
				style={'display': 'none'}, children = [task_in_progress])
			]

input_container0 = input_container_children()
idx_task_in_progress = len(input_container0) - 1 
idx1 = [ item.id=='filter_keyword' if hasattr(item, 'id') else False for item in input_container0]
idx_filter_keyword = np.array(range(len(input_container0)))[idx1][0]
idx2 = [ item.id=='picked_city' if hasattr(item, 'id') else False for item in input_container0]
idx_picked_city = np.array(range(len(input_container0)))[idx2][0]
idx3 = [ item.id=='picked_version' if hasattr(item, 'id') else False for item in input_container0]
idx_picked_version = np.array(range(len(input_container0)))[idx3][0]


input_names = ['picked_city', 'picked_version', 'picked_datetime',
				'picked_hour', 'filter_keyword', 'filter_submit']

hidden_data = ['hidden_latest_datatime', 'picked_city_extended', 
			 'picked_date_extended', 'stat_sentiments', 'stat_emotions',
			 'stat_words','top_tweets', 'top_users', 'stat_type',
			 'stat_triggered_context', 'previous_email_timestamp']

def subscribe_button(subscribe=True, email=''):
	if subscribe:
		style1 = {'padding': '0 20px'}
		style2 = {'display': 'none'}
	else:
		style1 = {'display': 'none'}
		style2 = {'padding': '0 20px'}

	return 	[dcc.Input(id='subscribe_email', type='text', value=email,
						placeholder='Enter an email address',
						debounce=True,
						style = {'minWidth': '50px'}
					),
			html.Button('Subscribe', id='button_subscribe',
						n_clicks = 0,
						style = style1),
			 html.Button('Unsubscribe', id='button_unsubscribe',
						n_clicks = 0,
						style = style2)
			 ]


app.layout = html.Div(
	children = [
		html.Div(
	        className="app-header",
	        children=[
		        html.Div("Tweets for #BlackLivesMatter", className="app-header--title"),
		        html.Div("Track the BLM movement with Twitter Data. ", className="app-header--subtitle"),
	        ]),
		dcc.Tabs(id='big-tabs', value='tab-dashboard', 
					children=[
			        	dcc.Tab(label='Dashboard', value='tab-dashboard'),
			        	dcc.Tab(label='Select Data', value='tab-datatab'),
			        	dcc.Tab(label='About', value='tab-about')
		    		]),
		html.Div(id='big-tabs-content',
    		style={'maxWidth': '960px', 'margin': 'auto'}
	    	),
		# App footer
		html.Div(
	        className="parent white",
	        children=[
		        html.Div(
			   	className="box1 section1",
			   	children = [
			   	html.Label('Subscribe to daily summary:', 
						style={'margin': '5px 10px 5px 0'}),
			   	html.Div(id='dynamic-button-container', children=subscribe_button()),
			   	html.Div(id='subscribe_note', children='')
			   	]),
			   	html.Div(
			   		children = [
			    		html.Div(className="sharethis-inline-share-buttons"),
		        		html.Iframe(srcDoc='<p style="color:#DCDCDC; text-align:right">&#169;-2020 Kota Minegishi</p>' +
		        			'<p style="color:#DCDCDC; text-align:right">Contact: BLMtracker2020@gmail.com</p>', 
		        			sandbox='',
		        			style={'align':'right', 'height': '35px', 'width':'500px', 
		        			'margin-top': '5px',
		        			'color':'#DCDCDC', 'border-width':'0px'}), 
			    	],
			    	className="box2 section2")
			   	]),
	    # Hidden components
	    dcc.Interval(
	            id='interval-component',
	            interval=600 * 1000, # in milliseconds
	            n_intervals=0
	        )
	    ] + # hidden data
	    [html.Div(id= name, style={'display': 'none'}, children = None) for name in hidden_data]
	)


# big tabs  
@app.callback(Output('big-tabs-content', 'children'),
              [Input('big-tabs', 'value')])
def render_big_tab_content(tab):
	if tab == 'tab-dashboard': 
		return html.Div(
			children = [
			dcc.Graph(id='dashboard') 
			],
			style = {'display': 'grid', 'place-items': 'center'}
			)
	elif tab == 'tab-datatab': 
		return html.Div(
			children = [
				html.Div(id='dynamic-input-container', 
					children=input_container_children(
						picked_datetime=str(latest_datatime_d_dt),
						picked_hour=latest_datatime_hour,
						task_in_progress=True),
					style = {'display': 'flex', 'flexFlow': 'row wrap', 'alignItems': 'center'}
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
			        	dcc.Tab(label='Top Users', value='tab-topusers')
		    		]),
		    	html.Div(id='tabs-content')
		   		]
	    	)
	elif tab == 'tab-about':
		return html.Div([
			dcc.Markdown(children=about_text1),
	    	html.Div(html.Img(src='assets/program_chart.png', style={'width':'75%'})),
	    	dcc.Markdown(children=about_text2)
        	])


# update selected tabs  
@app.callback(Output('tabs-content', 'children'),
              [Input('tabs', 'value')])
def render_content(tab):
	if tab is None: return None
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


def load_current_data():
	global current_data_cities, latest_datatime, latest_datatime_hour, latest_datatime_d_dt, app 

	#threading.Timer(600, load_current_data).start()
	print('Updating current data for evey 10 min:')

	result = client.invoke(FunctionName= 'BLM_current_data', # 'test_current_BLM_stat'
	                InvocationType='RequestResponse',                                      
	                Payload='{}')

	current_data_cities = json.loads(json.loads(result['Payload'].read()))
	df1 = pd.read_json(current_data_cities['all_v1']['stat_sentiments'], orient='split') 
	print('the latest data is for: ', pd.to_datetime(df1.time).max())
	latest_datatime = pd.to_datetime(df1.time).max()
	latest_datatime_hour = int(str(latest_datatime)[11:13])
	latest_datatime_d_dt = latest_datatime.floor('d').to_pydatetime()

	# note: using position index in app.layout and global 
	print('updating app layout default')
	# app.layout.children[1].children[0].children[4].max_date_allowed = str(latest_datatime_d_dt)
	# app.layout.children[1].children[0].children[4].date = str(latest_datatime_d_dt)
	# app.layout.children[1].children[0].children[5].value = latest_datatime_hour
	print(app.layout.children[1].children[1])
	print(app.layout.children[1].children[1].children)
	if len(app.layout.children[1].children[1])>1:
		app.layout.children[1].children[1].children[0].children[4].max_date_allowed = str(latest_datatime_d_dt)
		app.layout.children[1].children[1].children[0].children[4].date = str(latest_datatime_d_dt)
		app.layout.children[1].children[1].children[0].children[5].value = latest_datatime_hour

	# update current_city_stats 
	global current_city_stats 

	stats = []
	for city in cities + ['all_v1']:
	    df = pd.read_json(current_data_cities[city]['stat_sentiments'], orient='split')
	    df.time = pd.to_datetime(df.time)
	    
	    # today
	    df_today = df[df.time.apply(lambda x: latest_datatime.floor('d') == x.floor('d'))]

	    # yesterday
	    df_yesterday = df[df.time.apply(lambda x: latest_datatime.floor('d') - pd.Timedelta(1,unit='d') == x.floor('d'))]

	    # past 7 days
	    df_seven = df[latest_datatime - df.time <= pd.Timedelta(7, unit='d')]
	    city_stats = (city, wAvg(df_today), wAvg(df_yesterday), wAvg(df_seven), wAvg(df))
	    stats.append(city_stats)

	stats = pd.DataFrame(stats, columns=['city', 'today', 'yesterday', 'seven_days', 'two_weeks'])
	stats.loc[stats.city=='all_v1','city'] = 'All Cities'
	stats = stats.sort_values(by='city', ascending=False)

	avg = float(stats.loc[stats.city=='All Cities','today'])

	stats['diff_from_all'] = round(stats.today - avg, 1)
	stats['ch_from_yest'] = round(stats.today - stats.yesterday, 1)
	stats['ch_from_sevend'] = round(stats.today - stats.seven_days, 1)
	stats['ch_from_twoweek'] = round(stats.today - stats.two_weeks, 1)
	current_city_stats = stats
 
load_current_data()


# periodic data updates
@app.callback(
	Output('hidden_latest_datatime', 'children'),
	[Input('interval-component','n_intervals')])
def update_current_data(n_intervals):
	load_current_data()
	return latest_datatime


# update options for picked_hour 
@app.callback(
	Output('picked_hour','options'),
	[Input('stat_type','children')],
	[State('picked_datetime', 'date')]
	)
def set_hour_to_max(stat_type, date):
	#print('In set_hour_to_max()')
	max_hour = latest_datatime_hour+1 if str(date)[:10] == str(latest_datatime_d_dt)[:10] else 24
	return [{'label': str(h) + ':00', 'value':h} for h in range(max_hour)]


# check and adjust date if it is before 7/1 
@app.callback(
	[Output('picked_date_extended','children'),
	 Output('picked_city', 'value'),
	 Output('picked_version', 'value')
	],
	[Input('picked_datetime', 'date')],
	[State('picked_city', 'value'),
	 State('picked_version', 'value'),
	 State('task_in_progress', 'children')]
	)
def date_check(date, city, version, task_in_progress):
	print('In date_check(): checking to use archived data for ', str(date)[:10])
	if task_in_progress[0]==False: PreventUpdate()
	date = pd.to_datetime(date)
	if date <= pd.to_datetime('2020-07-01'):
		absdiff = [date - t if date - t >= pd.Timedelta(0, unit='h') else t - date 
					 for t in archive_dates]
		date2 = archive_dates[absdiff.index(min(absdiff))].to_pydatetime()
		city2 = 'all'
		version2 = 1
	else:
		date2 = date 
		city2 = city
		version2 = version
	return [date2, city2, version2]


# update timespan lists
for item_id in ['topwords_timespan', 'topusers_timespan', 'toptweets_timespan']:
	@app.callback(
		Output(item_id,'options'),
		[Input('picked_datetime', 'date')]
		)
	def update_topwords_timespan(date):
		return list_timespan if date == str(latest_datatime_d_dt) else list_timespan1


# show/hide a version for all cities random sample 
@app.callback(
	[Output('picked_city_extended','children'),
	Output('picked_version', 'style')],
	[Input('picked_city', 'value'),
	Input('picked_version','value')]
	)
def city_extended0(city, version):
	print('In city_extended0():', city, version)
	if city=='all':
		city_extended = city + '_v' + str(version) 
		return [city_extended, {'min-width': '100px', 'display': 'flex'}]
	else:
		return [city, {'display': 'none'}]


# generate a note for the user on what the app is doing
@app.callback(
	Output('note_filter','children'),
	[Input('task_in_progress', 'children'),
	 Input('filter_keyword','value'),
	 Input('filter_submit', 'n_clicks'),
	 Input('picked_city_extended', 'children'),
	 Input('picked_datetime','date')],
	 [State('picked_city','value'),
	 State('picked_version','value')]
	)
def update_note_filter(task_in_progress, filter_keyword, n_clicks, 
	city_extended, date, city, version):
	context = dash.callback_context.triggered[0]['prop_id'].split('.')[0]
	if context =='': return None
	print('updating note by trigger', context) 
	if context=='filter_keyword':
		str1 = 'Please note: filtering data <b>will take some time</b>. Press <b>"Filter"</b> to proceed.'
	elif context=='filter_submit':
		str1 = '<b>Please wait</b>: app is accessing <b>raw data</b>, filtering, and re-calculating statistics... This takes time.'
	elif task_in_progress[0]:
		str1 = '<b>Please wait</b>: app is loading statistics...'
	else:
		return None
	return [html.Iframe(srcDoc=str1, sandbox='',
			style={'height': '30px', 'width':'950px', 
				'margin': '0px 2px 2px 0px', 'borderRadius': '5px',
	    		'padding': '5px', 
	    		'backgroundColor': '#E1FFDC',
	    		'borderStyle': 'hidden',})]


# generate a note for the user about the currently loaded data
@app.callback(
	Output('selected_data_description','children'),
	[Input('stat_type','children')],
	[State('picked_city', 'value'),State('picked_datetime','date'),
	State('picked_hour','value'), State('filter_keyword','value'),
	State('picked_version','value'), State('picked_date_extended','children')]
	)
def return_data_descripton(stat_type, city, date, hour, filter_keyword, picked_version, date_extended):
	city_str = 'All Cities' if city=='all' else city_name_add_space(city)
	if city=='all': city_str = city_str + ' (2% data sample Version <b>' + str(picked_version) + ')</b>'
	str1 = 'Selected data are for <b>' + city_str + '</b> on <b>' + str(date_extended)[:10] + '</b>, <b>' + str(hour) + ':00 CST</b>' 
	print(stat_type)
	prev_type = stat_type if stat_type is not None else 'none'
	if prev_type == 'filtered stats': 
		str1 = str1 + ', filtered for tweets containing word "<b>' + filter_keyword + '</b>"'
	str1 = str1 +'.'
	if pd.to_datetime(date) < pd.to_datetime('2020-07-01'): str1 = str1 + ' Note: the nearest archived date available is used.' 
	return html.Iframe(srcDoc=str1, sandbox='',
		style={'height': '30px', 'width':'950px', 
			'margin': '0px 2px 2px 0px', 'borderRadius': '5px',
    		'padding': '5px', 
    		'backgroundColor': '#DCE9FF',
    		'borderStyle': 'hidden',})


# disable user inputs while processing data
@app.callback(
	[Output(inputid,'disabled') for inputid in input_names],
	[
	# Input('filter_submit','n_clicks'),
	# Input('picked_city_extended','children'),
	# Input('picked_datetime', 'date'),
	Input('task_in_progress', 'children')]
	)
def disable_inputs(#n_clicks, city, date, 
	task_in_progress):
	#context = dash.callback_context.triggered[0]['prop_id'].split('.')[0]
	contexts = [x['prop_id'].split('.')[0] for x in dash.callback_context.triggered]
	if (contexts ==['']) | (task_in_progress[0]==False): return [False for i in input_names]
	print('disabling inputs while processing data, trigger:', contexts)
	return [True for i in input_names]	


@app.callback(
	[Output('task_in_progress', 'children'),
	Output('stat_triggered_context', 'children')
	],
	[
	Input('picked_city_extended','children'),
	Input('picked_date_extended','children'),
	Input('filter_submit','n_clicks'),
	Input('picked_hour','value')]
	)
def set_task_in_progress(city, date, filter, hour):
	print('In set_task_in_progress()')
	context = dash.callback_context.triggered[0]['prop_id'].split('.')[0]
	return [[True], context]


# load selected statistics 
@app.callback(
	[
	Output('stat_sentiments','children'),
	Output('stat_emotions','children'),
	Output('stat_words','children'),
	Output('top_tweets','children'),
	Output('top_users','children'),
	Output('stat_type','children'),
	Output('dynamic-input-container', 'children'),
	Output('dynamic-note-container', 'children')],
	[Input('task_in_progress', 'children')],
	[
	State('stat_triggered_context', 'children'),
	State('picked_city_extended','children'),
	State('picked_date_extended','children'),
	State('picked_hour','value'),
	State('filter_keyword','value'),
	State('picked_city','value'),
	State('picked_version','value'),
	State('dynamic-input-container', 'children'),
	State('dynamic-note-container', 'children'),
	State('stat_sentiments','children'),
	State('stat_emotions','children'),
	State('stat_words','children'),
	State('top_tweets','children'),
	State('top_users','children'),
	State('stat_type','children'),
	]
	) 
def pick_stat_city_date(
	task_in_progress, stat_triggered_context, city, date, hour,
	filter_keyword, city0, city_all_version, input_container, note_container, 
	stat_sentiments, stat_emotions, stat_words, top_tweets, top_users, stat_type):
	
	print('In pick_stat_city_date():')
	print('selected city date hour: ' + city + ' ' + str(date)[:10] + ' ' + str(hour) + ':00')
	
	print('triggered context:', stat_triggered_context) 

	if stat_type is not None:
		# skip to showing stats for the selected hour
		prev_type = stat_type
		print('stat_type: ',stat_type)
		if (prev_type != 'current stats') & (stat_triggered_context == 'picked_hour'):
			print('skipping picked_stats update')
			set_task_in_progress_false(input_container, idx_task_in_progress, children=[False]) 
			return [stat_sentiments, stat_emotions, stat_words, top_tweets, 
			top_users, stat_type, input_container, note_container]

	# re-create note 
	note_container2 = [html.Div(id='note_filter', children=[])]

	if (str(date)[:10] == str(latest_datatime_d_dt)[:10]) & (filter_keyword == '') & (str(hour)==str(latest_datatime_hour)):
		# load current stats 
		print('Returning current stats for ' + city)
		stats = current_data_cities[city]
		stats['type'] = 'current stats'
		clear_filter_keyword(input_container, idx_filter_keyword)
		set_task_in_progress_false(input_container, idx_task_in_progress, children=[False]) 
		return stat_list(stats) + [input_container, note_container2]

	# data to pass to AWS Lambda function
	data = {'city': city, 'date': date[:10], 
		'filter_keyword': filter_keyword}
	
	# re-create input controls 
	input_container2 = input_container_children(
		picked_city=city0, picked_version=city_all_version,
		picked_datetime=date, picked_hour= hour,
		filter_keyword=filter_keyword, n_clicks=0)
	set_picked_version_style(input_container2, idx_picked_city, idx_picked_version)
	
	if (stat_triggered_context=='filter_submit') & (filter_keyword!=''):
		# filter data and re-calculate stats
		print('Getting stats for ' + city + ' on ' + date[:10] + ' with filter: ', filter_keyword)
		result = client.invoke(FunctionName='BLM_stats',
	                InvocationType='RequestResponse',                                      
	                Payload=json.dumps(data))

	else:
		# retrieve stats 
		print('Getting stats for ' + city + ' on ' + date[:10])
		result = client.invoke(FunctionName='BLM_get_stats',
	                InvocationType='RequestResponse',                                      
	                Payload=json.dumps(data))
		clear_filter_keyword(input_container2, idx_filter_keyword)


	print(result)
	stats = json.loads(json.loads(result['Payload'].read()))
	return stat_list(stats) + [input_container2, note_container2]



'''
	callback functions to update figures and tables
'''
@app.callback(
	Output('dashboard','figure'),
	[Input('interval-component','n_intervals')]
	)
def update_fig_dashboard(n_intervals):
	return fig_dashboard(current_city_stats, latest_datatime)


@app.callback(
    Output('sentiments', 'figure'),
    [Input('stat_sentiments','children')]
    )
def update_fig_sentiments(picked_stats):
	if picked_stats is None: return None
	print('IN update_fig_sentiments():')
	df = pd.read_json(picked_stats, orient='split',
		convert_dates=['time'], date_unit='ms')
	# print(df)
	return fig_sentiments(df)

@app.callback(
    Output('emotions', 'figure'),
    [Input('stat_emotions','children')]
    )
def update_fig_emotions(picked_stats):
	if picked_stats is None: return None
	print('IN update_fig_emotions():')
	df = pd.read_json(picked_stats, orient='split',
		convert_dates=['time'], date_unit='ms'
		)
	return fig_emotions(df)


@app.callback(
    Output('word_bars', 'figure'),
    [Input('stat_words','children'),
    Input('topwords_timespan','value'),
    Input('picked_hour','value')]
    )
def update_fig_word_bars(picked_stats, timespan, hour):
	if picked_stats is None: return None
	print('IN update_fig_word_bars():')
	df = pd.read_json(picked_stats, orient='split')
	#print(df)
	timespan2 = get_timespan(hour, timespan)
	return fig_word_bars(df, subset=timespan2)


@app.callback(
     Output('plt_word_cloud', 'src'),
    [Input('stat_words','children'),
    Input('topwords_timespan','value'),
    Input('picked_hour','value')]
    )
def update_word_cloud(picked_stats, timespan, hour):
	if picked_stats is None: return None
	print('IN update_word_cloud():')
	df = pd.read_json(picked_stats, orient='split')
	timespan2 = get_timespan(hour, timespan)
	fig, ax1 = plt.subplots(1,1)
	fig = fig_word_cloud(df, subset=timespan2)
	out_url = fig_to_uri(fig)
	return out_url


@app.callback(
    Output('users_bars', 'figure'),
    [Input('top_users','children'),
    Input('topusers_timespan','value'),
    Input('picked_hour','value')]
    )
def update_fig_users_bars(picked_stats, timespan, hour):
	if picked_stats is None: return None
	print('IN update_fig_users_bars():')
	df = pd.read_json(picked_stats, orient='split')
	fix_user_id(df)
	#print(df)
	timespan2 = get_timespan(hour, timespan)
	return fig_top_users(df, subset=timespan2)


@app.callback(
    Output('pass_tbl_top_tweets', 'children'),
    [Input('top_tweets','children'),
    Input('toptweets_timespan','value'),
    Input('picked_hour','value')]
    )
def update_tweet_table(picked_stats, timespan, hour):
	if picked_stats is None: return None
	print('IN update_tweet_table():')
	df = pd.read_json(picked_stats, orient='split')
	fix_RT_id(df)
	#print(df)
	timespan2 = get_timespan(hour, timespan)
	return gen_dash_table('tbl_top_tweets', df, 
		subset=timespan2, type='top_tweets')

@app.callback(
    Output('pass_tbl_top_users', 'children'),
    [Input('top_users','children'),
    Input('topusers_timespan','value'),
    Input('picked_hour','value')]
    )
def update_user_table(picked_stats, timespan, hour):
	if picked_stats is None: return None
	print('IN update_user_table():')
	df = pd.read_json(picked_stats, orient='split')
	fix_user_id(df)
	#print(df)
	timespan2 = get_timespan(hour, timespan)
	return gen_dash_table('tbl_top_users', df, 
		subset=timespan2, type='top_users')


'''
	email subscription for daily summary
'''

def get_daily_dashboard():
	url = py.plot(fig_dashboard(current_city_stats, latest_datatime), 
		auto_open=False, 
		filename='dashboard_{date}'.format(date= str(latest_datatime_d_dt)[:10]))
	return url

def get_daily_top_users():
	df = pd.read_json(current_data_cities['all_v1']['top_users'], orient='split') 
	fix_user_id(df)
	url = py.plot(fig_top_users(df, 'today'),
		auto_open=False, 
		filename='top_users_{date}'.format(date= str(latest_datatime_d_dt)[:10]))
	return url

def get_daily_top_tweets():
	top_tweets = pd.read_json(current_data_cities['all_v1']['top_tweets'], orient='split')
	formatInt(top_tweets, ['followers_count', 'retweet_timespan', 'retweet_total'])

	tt1 = (top_tweets[top_tweets.subset=='today'][['user_name','followers_count','text','t_co',
                                              'retweet_timespan','retweet_total']]
       .rename(columns = {'user_name': 'User Name', 'followers_count':'Followers',
                         'text':'Tweets', 't_co':'Link', 'retweet_timespan':'Retweets in DB', 
                          'retweet_total':'Retweets Total'})
      )

	tt1_html = (tt1.to_html(index=False)
			 .replace('\n',' ')
             .replace('<table border="1" class="dataframe">',
             	'<table align="center" width="600" border="1" rules="rows" bordercolor="#e0e2e2" cellspacing="0" cellpadding="2" bgcolor="#ffffff">')
             .replace('<tr style="text-align: right;">', 
                     '<tr style="text-align: center; background-color: #b8c9ca;">')   
            )
	return tt1_html


# send daily summary around 16:00 CTS
@app.callback(
	Output('previous_email_timestamp','children'),
	[Input('interval-component','n_intervals')]
	)
def send_daily_summary(n_intervals):
	print('In send_daily_summary()')
	# temporarily disable the following for testing 
	if latest_datatime_hour != 16: return ''
	#print(latest_datatime_hour)

	conn = sqlite3.connect(db)
	c = conn.cursor()
	c.execute("select datetime from blm_daily_log")
	result = c.fetchall()
	conn.close()
	previous_email = result.pop()[0]
	print(previous_email)

	date = str(latest_datatime_d_dt)[:10]

	if previous_email[:10] == date: return ''

	email_body = daily_template.format(
		note = '',
		date = date, 
		url_dashboard = get_daily_dashboard(),
		url_top_users = get_daily_top_users(),
		html_top_tweets = get_daily_top_tweets()
	) 
	conn = sqlite3.connect(db)
	conn.row_factory = sqlite3.Row 
	c = conn.cursor()
	c.execute("SELECT * from blm_daily_email")
	results = c.fetchall()
	conn.close()
	emails = [result['email'] for result in results]
	print('Sending daily emails to:', emails)
	for email in emails:
		try: 
			yag.send(str(email), 
	        	 'BLM Tracker Daily Summary - {date}'.format(date = date), 
	        	 email_body)
			print('daily email sent to:', str(email))
		except:
			print('failed: daily email to:', str(email))
	current_time = (dt.utcnow() + pd.DateOffset(hours=-6)).strftime('%Y-%m-%d %H:%M:%S')
	conn = sqlite3.connect(db)
	c = conn.cursor()
	c.execute("INSERT INTO blm_daily_log VALUES (?, ?)",
    (str(emails), current_time))
	conn.commit()
	conn.close()
	return str(current_time)



# show/hide subscribe/unsubscribe buttons 
@app.callback(
	[Output('button_subscribe', 'style'),
	Output('button_unsubscribe', 'style'),
	],
	[Input('subscribe_email', 'value')]
	)
def subscribe_display(email):
	print('In subscribe_button():')
	if (str(email).find('@')==-1) | (str(email).find('.')==-1):
		return [{'padding': '0 20px'}, {'display': 'none'}]
	conn = sqlite3.connect(db)
	c = conn.cursor()
	c.execute("SELECT * from blm_daily_email where email =='{email}'".format(email=str(email)))
	result = c.fetchall()
	conn.close()
	if len(result) >0:
		return [{'display': 'none'}, {'padding': '0 20px'}]
	else:
		return [{'padding': '0 20px'}, {'display': 'none'}]


# disable subscribe/unsubscribe buttons
@app.callback(
	[Output('button_subscribe','disabled'),
	Output('button_unsubscribe','disabled'),
	Output('subscribe_email','disabled')],
	[Input('button_subscribe','n_clicks'),
	Input('button_unsubscribe','n_clicks')]
	)
def disable_subscribe_button(n_clicks1, n_clicks2):
	if max(n_clicks1, n_clicks2) > 0:
		print('disabling subscribe button')
		return [True, True, True]
	return [False, False, False]



# subscribe to/unsubscribe from daily summary
@app.callback(
	[Output('subscribe_note','children'),
	Output('dynamic-button-container', 'children')],
	[Input('button_subscribe','n_clicks'),
	Input('button_unsubscribe','n_clicks')],
	[State('subscribe_email','value')]
	)
def subscribe(n_clicks1, n_clicks2, email):
	if ((n_clicks1>0) | (n_clicks2>0)) & ((str(email).find('@')==-1) | (str(email).find('.')==-1)):
		return ['Please provide a valid email address.', subscribe_button(email=email)]
	context = dash.callback_context.triggered[0]['prop_id'].split('.')[0]
	if context =='button_subscribe':
		current_time = (dt.utcnow() + pd.DateOffset(hours=-6)).strftime('%Y-%m-%d %H:%M:%S')
		conn = sqlite3.connect(db)
		c = conn.cursor()
		c.execute("INSERT INTO blm_daily_email VALUES (?, ?)", (str(email), current_time))
		conn.commit()
		conn.close()
		date = current_time[:10]
		email_body = daily_template.format(
			note = 'This email confirms that you are subscribed to BLM Tracker Daily Summary.' +
			' The following is a sample of daily summary.',
			date = date, 
			url_dashboard = get_daily_dashboard(),
			url_top_users = get_daily_top_users(),
			html_top_tweets = get_daily_top_tweets()
		) 
		try: 
			yag.send(str(email), 
	        	 'BLM Tracker Subscription Confirmation - {date}'.format(date = date), 
	        	 email_body)
		except:
			pass
		return [str(email) + ' is now subscribed. Please check your email.', subscribe_button()]
	
	elif context == 'button_unsubscribe':
		conn = sqlite3.connect(db)
		c = conn.cursor()
		try:
			c.execute("DELETE from blm_daily_email where email =='{email}'".format(email=str(email)))
			conn.commit()
		except:
			pass
		conn.close()
		yag.send(str(email), 
	        	 'BLM Tracker Unsubscription Confirmation', 
	        	 'This email confirms that you are unsubscribed from BLM Tracker Daily Summary.')

		return [str(email) + ' is now unsubscribed. Please check your email.', subscribe_button()]
	else: return ['', subscribe_button()]


if __name__ == '__main__':
	port = int(os.environ.get('PORT', 8050))
	app.run_server(host='0.0.0.0', port=port, debug=True) #, dev_tools_ui=False) #, dev_tools_props_check=False)
	
