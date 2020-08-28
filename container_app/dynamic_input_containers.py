import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import pandas as pd
from datetime import datetime as dt 

from parameters import list_cities, cities_all

class InputContainerMain:
	def __init__(
		self,
		picked_city='all', picked_version=1,
		picked_datetime=str(pd.to_datetime('2020-07-01')),
		max_date_allowed=str(pd.to_datetime('2020-12-31')),
		picked_hour= 16,
		filter_keyword='', n_clicks=0,
		task_in_progress=False):

		self.container = [
			html.Label('Select City:', 
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
		        max_date_allowed=max_date_allowed,
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
				style={'display': 'none'}, children = task_in_progress)
			]
		self.assign_index()

	def assign_index(self):
		self.idx = {} 
		for i, item in enumerate(self.container): 
			if hasattr(item, 'id'): 
				self.idx[item.id] = i

	def set_picked_version_style(self):
	    pos1 = self.idx['picked_city']
	    pos2 = self.idx['picked_version'] 
	    if 'props' in self.container[pos1]:
	        city = self.container[pos1]['props']['value']
	        if city=='all': 
	            self.container[pos2]['props']['style'] = {'min-width': '100px', 'display': 'flex'}
	        else:
	            self.container[pos2]['props']['style'] = {'display': 'none'}
	    elif hasattr(self.container[pos1],'value'):
	        city = self.container[pos1].value 
	        if city=='all': 
	            self.container[pos2].style = {'min-width': '100px', 'display': 'flex'}
	        else:
	            self.container[pos2].style = {'display': 'none'}


# def clear_filter_keyword(input_container):
# 	    pos = [i for i, item in enumerate(input_container) if getattr(item, 'id', '_na')=='filter_keyword'][0]
# 	    if 'props' in input_container[pos]:
# 	        input_container[pos]['props']['value'] = ''
# 	    elif hasattr(input_container[pos],'value'):
# 	        input_container,[pos].value = ''

def set_task_in_progress_false(input_container, in_progress=False):
    pos =  [i for i, item in enumerate(input_container) if item['props'].get('id', '_na')=='task_in_progress'][0]
    input_container[pos]['props']['children'] = in_progress



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


