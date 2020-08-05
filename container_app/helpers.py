import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from collections import Counter
from wordcloud import WordCloud, ImageColorGenerator
import matplotlib.pyplot as plt
from PIL import Image
import dash_table
import copy
import itertools 
from io import BytesIO
import base64
from glob import glob 
from math import isnan
import dash_html_components as html

from summarizing_helpers import *


'''
	misc unctions 
'''

def city_name_add_space(name):
    caps = [i for i,c in enumerate(name) if c.isupper()]
    i = 0
    for l in caps[1:]:
        name = name[:(l+i)] + ' ' + name[(l+i):]
        i = i+1
    return name

def city_value(city_label_value):
    return city_label_value['value']

def city_date(city, date):
	return str(city) + '#' + str(date)


def clear_filter_keyword(input_container, pos):
	input_container[pos]['props']['value'] = ''


'''
	functions to process data
'''




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

def convert_ints(df, int_dtype='int32'):
    ints = df.select_dtypes(include=['int64']).columns.tolist()
    df[ints] = df[ints].astype(ints_dtype)
    return df

def tw_data_files_to_df_csv(files, float_dtype='float32'):
    '''append and concat data files into a pandas.DataFrame'''
    df = []
    [ df.append(pd.read_csv(file)) for file in files ]
    df = pd.concat(df, ignore_index=True)
    if float_dtype is None: return df
    return convert_floats(df, float_dtype)


def tw_data_files_to_df_json(files, lines=False, float_dtype='float32'):
    '''append and concat data files into a pandas.DataFrame'''
    df = []
    [ df.append(pd.read_json(file, orient='records', lines=lines)) for file in files ]
    df = pd.concat(df, ignore_index=True)
    if float_dtype is None: return df
    return convert_floats(df, float_dtype)


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

def keep_by_matched_id(df, list_id, varname='id'):
    return (df.set_index(varname)
            .join(pd.DataFrame(data={varname: list_id}).set_index(varname), how='inner')
            .reset_index()
            )
    
def get_stats(cum_original, cum_retweet, cum_words, 
	cum_sentiments, cum_emotions, time=None):
	print('In get_stats()')
	stat_sentiments = calc_stat_sentiments(cum_sentiments)
	stat_emotions = calc_stat_emotions(cum_emotions)
	del cum_sentiments, cum_emotions

	cum_data = cumulative_data(cum_ori = cum_original, 
	                      cum_rt = cum_retweet,
	                      cum_words = cum_words,
	                      now = time
	                      )
	del cum_original, cum_retweet, cum_words

	cum_data.add_words_subsets()
	cum_data.add_tweet_subsets()
	cum_data.add_user_subsets() 

	stat_words = cum_data.stat_words
	top_tweets = cum_data.top_tweets
	top_users = cum_data.top_users 

	return stat_sentiments, stat_emotions, stat_words, top_tweets, top_users



def filter_data(filter_word, cum_original, cum_retweet, cum_words, 
	cum_sentiments, cum_emotions, time=None):
    # define filtered data of cum_data 
    # filter_word = 'protest'
    print('In filter_data():')
    cum_original2, cum_retweet2, cum_words2 = filter_main(filter_word,
    	cum_original, cum_retweet, cum_words)
    
    # print(cum_words2)
    cum_sentiments2 = filter_sentiments(cum_sent = cum_sentiments, 
                                        ori_filtered = cum_original2)
    cum_emotions2 = filter_sentiments(cum_sent = cum_emotions, 
                                        ori_filtered = cum_original2)    
    
    return get_stats(cum_original2, cum_retweet2, cum_words2, cum_sentiments2, cum_emotions2, time)


def filter_sentiments(cum_sent, ori_filtered):
	print('IN filter_sentiments():')

	sent_labels = [*cum_sent.columns][2:]
	# print(sent_labels)
	cum_sent2 = (cum_sent.set_index('id')
                   .join(ori_filtered.set_index('id'), rsuffix = '_ORI', how='inner')
                   .reset_index()
                   )[['id', 'created_at_h', *sent_labels]]
	return cum_sent2


def filter_main(filter_word, cum_original, cum_retweet, cum_words):
	''' filtered data are sequentially defined for retweet, original, words dataset'''
	print('IN filter_datasets():')
	idx_a =  cum_retweet.tokens.apply(lambda x: filter_word in x)
	cum_retweet2 = cum_retweet[idx_a]
	#print(sum(idx_a))

	idx_b = cum_original.tokens.apply(lambda x: filter_word in x)
	match_b = (cum_original.set_index('RT_id')
	        .join(cum_retweet2.set_index('RT_id'), rsuffix = '_RT', how='inner')
	       )
	cum_original2 = (match_b.reset_index()
	                 .append(cum_original[idx_b])
	                 .drop_duplicates(subset=['id'])
	                 .reset_index()
	                )
	#print(sum(idx_b),len(cum_original2))

	cum_words2 = (cum_words.set_index('id')
		.join(cum_original2.set_index('id'), rsuffix = '_ORI', how='inner')
		.reset_index()
		)

	#print(len(cum_words2))

	return cum_original2, cum_retweet2, cum_words2




def get_columns_json(file):
    chunk1 = pd.read_json(file, chunksize=1, orient='records', lines=True)
    for d in chunk1:
        data1 = d.iloc[0]
        break
    return list(data1.keys())

def get_columns_csv(file):
    chunk1 = pd.read_csv(file, chunksize=1)
    return list(chunk1.read(1).keys())


def load_null_df(data_path):
    
    null_cum_sentiments = pd.DataFrame(columns = get_columns_csv(
                     glob(data_path + 'data_cumulative/sentiments/*')[0]))
    
    null_cum_emotions = pd.DataFrame(columns = get_columns_csv(
                     glob(data_path + 'data_cumulative/emotions/*')[0]))
    
    null_cum_words = pd.DataFrame(columns = get_columns_json(
                     glob(data_path + 'data_cumulative/words/*')[0]))
    
    null_cum_original = pd.DataFrame(columns = get_columns_json(
                     glob(data_path + 'data_cumulative/original/*')[0]))
    
    null_cum_retweet = pd.DataFrame(columns = get_columns_json(
                     data_path + 'data_cumulative/retweet/2020_all_retweets.json'))
    
    return null_cum_sentiments, null_cum_emotions, null_cum_words, null_cum_original, null_cum_retweet



'''
	functions to generate plots
'''



no_data_fig = {"layout": {
        "xaxis": {
            "visible": False
        },
        "yaxis": {
            "visible": False
        },
        "annotations": [
            {
                "text": "No matching data found.",
                "xref": "paper",
                "yref": "paper",
                "showarrow": False,
                "font": {
                    "size": 20
                }
            }
        ]
    }
}

def get_timespan(hour, timespan):
	return 'hour_' + str(hour) if timespan == 'now_1h' else timespan



def fig_sentiments(df):
	if len(df)==0: return no_data_fig

	colors = ['#ff5733', '#33dbff']
	line_size = [5,5]
		
	# Create figure with secondary y-axis
	fig = make_subplots(specs=[[{"secondary_y": True}]])

	# Add traces
	fig.add_trace(
		go.Scatter(x=df.time, 
			y=df['mean'], name="Sentiments",
			line=dict(color=colors[0], width=line_size[0]),
			connectgaps=True),
		secondary_y=False,
		)

	fig.add_trace(
		go.Scatter(x=df.time,
			y=df['count'], name="Tweets",
			line=dict(color=colors[1], width=line_size[1]),
			connectgaps=True),
		secondary_y=True,
		)


	# Set y-axes titles
	fig.update_yaxes(title_text="<b>Average Sentiments</b>", secondary_y=False, color= colors[0],
		showgrid=True, gridwidth=1, gridcolor= '#ffbbb8',
		zeroline=True, zerolinewidth=2, zerolinecolor='#ffbbb8')
	fig.update_yaxes(title_text="<b>Number of Tweets</b>", secondary_y=True, color= colors[1],
		showgrid=True, gridwidth=1, gridcolor= '#b8fffa')

	# Add x-asis range selector 
	fig.update_xaxes(
		rangeslider_visible=True,
		rangeselector=dict(
			buttons=list([
	            dict(count=1, label="1d", step="day", stepmode="backward"),
	            dict(count=7, label="1w", step="day", stepmode="backward"),
	            dict(count=14, label="2w", step="day", stepmode="backward"),
	            # dict(count=1, label="1m", step="month", stepmode="backward"),
	            # dict(count=6, label="6m", step="month", stepmode="backward"),
	            # dict(count=1, label="YTD", step="year", stepmode="todate"),
	            # dict(count=1, label="1y", step="year", stepmode="backward"),
	            dict(step="all")
				])
			)
		)

	fig.update_layout(
		title_text='<b>Tweets</span></b> and <b>Sentiments</b>',
		plot_bgcolor='white',
		showlegend=False,
    )
	#fig.show()
	return fig



def fig_emotions(df):
	if len(df)==0: return no_data_fig

	x_data = df.time
	y_data = df[['fear','anger','trust','surprise','sadness','disgust','joy']]

	names = ['fear','anger','trust','surprise','sadness','disgust','joy']
	colors = {'fear':'#F3722C','anger':'#F94144','trust':'#43AA8B','surprise':'#F9C74F',
	          'sadness':'#577590','disgust':'#F8961E','joy':'#90BE6D'}
	line_size = 4

	fig = make_subplots()

	# Add traces
	for name in names:
	    fig.add_trace(
	        go.Scatter(x=x_data, y=y_data[name], name=name,
	               line=dict(color=colors[name], width=line_size),
	               connectgaps=True)
	    )

	# Set y-axes titles
	fig.update_yaxes(title_text="<b>Average Emotion Indicators</b>", #color= colors[0],
	                showgrid=True, gridwidth=1, gridcolor= '#E6E3DC',
	                 zeroline=True, zerolinewidth=2, zerolinecolor='#E6E3DC')

	# Add x-asis range selector 
	fig.update_xaxes(
	    rangeslider_visible=True,
	    rangeselector=dict(
	        buttons=list([
	            dict(count=1, label="1d", step="day", stepmode="backward"),
	            dict(count=7, label="1w", step="day", stepmode="backward"),
	            dict(count=14, label="2w", step="day", stepmode="backward"),
	            # dict(count=1, label="1m", step="month", stepmode="backward"),
	            # dict(count=6, label="6m", step="month", stepmode="backward"),
	            # dict(count=1, label="YTD", step="year", stepmode="todate"),
	            # dict(count=1, label="1y", step="year", stepmode="backward"),
	            dict(step="all")
	        ])
	    )
	)

	fig.update_layout(
	    title_text='<b>Emotions</span></b> in Tweets',
	    plot_bgcolor='white',
	    legend_orientation="h",
	    legend=dict(
	        font_size=14,
	        x = 0,
	        y = 1.085,
	        
	    ),
	)

	#fig.show()
	return fig



#from io import BytesIO
#import base64
def fig_to_uri(in_fig, close_all=True, **save_args):
    # https://github.com/4QuantOSS/DashIntro/blob/master/notebooks/Tutorial.ipynb
    # type: (plt.Figure) -> str
    """
    Save a figure as a URI
    :param in_fig:
    :return:
    """
    out_img = BytesIO()
    in_fig.savefig(out_img, format='png', **save_args)
    if close_all:
        in_fig.clf()
        plt.close('all')
    out_img.seek(0)  # rewind file
    encoded = base64.b64encode(out_img.read()).decode("ascii").replace("\n", "")
    return "data:image/png;base64,{}".format(encoded)


def fig_word_cloud(stat_words, subset='now_1h', max_num=200):
	print('IN fig_word_cloud():')
	if sum(stat_words.subset==subset)==0: return plt.figure(figsize=[1,1])
	words_dict = stat_words[stat_words.subset==subset].token_counter.iloc[0]
	if words_dict =={}: return plt.figure(figsize=[1,1])

	custom_mask = np.array(Image.open("twitter_bird.png"))
	wc = WordCloud(background_color="white", mask=custom_mask)
	wc.generate_from_frequencies(dict(Counter(words_dict).most_common(max_num)))
	plt.figure(figsize=[10,10])
	plt.imshow(wc, interpolation="bilinear")
	plt.axis("off")
	#plt.show()
	return plt


def fig_word_bars(stat_words, subset='now_1h', num=15):
	if sum(stat_words.subset==subset)==0: return no_data_fig
	words_dict = stat_words[stat_words.subset==subset].token_counter.iloc[0]
	if words_dict =={}: return no_data_fig

	words_dict15 = Counter(words_dict).most_common(num)
	words_dict15 = dict(words_dict15)

	df_words_dict = pd.DataFrame.from_dict(
	    data = {'word':list(words_dict15.keys()), 'count':list(words_dict15.values())},
	    orient = 'columns'
	)

	#import plotly.express as px
	fig = px.bar(df_words_dict.sort_values(by=['count']),
	             x='count', y='word', text='count', orientation='h')
	fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
	fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
	#fig.show()
	return fig 


def clean_top_tweets(top_tweets):
	df = (top_tweets[['user_name','followers_count','text',
	  'retweet_timespan', 'retweet_total','tags','t_co']]
	  ).rename(columns={
	  'user_name':'User Name', 
	  'followers_count':'Followers',
	  'text':'Tweet',
	  'retweet_timespan':'Retweets Selected',
	  'retweet_total':'Retweets Total',
	  'tags':'Tags','t_co':'URL'})
	#print(df)

	df['User Name'] = df['User Name'].apply(lambda x: 
		'[' + str(x) + '](https://twitter.com/'+  str(x) + ')'
	)

	df['Tweet'] = df['Tweet'].apply(lambda x: 
		x.replace('"','').replace("'",'')
		)

	df['Tags'] = df['Tags'].apply(lambda x:
	 str(x).replace('[','').replace(']','').replace("'",'')
	 .replace('.',' ').replace(',',' ').replace("#BlackLivesMatter",'')
	 )

	df['URL'] = df['URL'].apply(lambda x:
		'[' + str(x) + '](' +  str(x) + ')'
		)
	return df

col_widths_toptweets = [
	        {'if': {'column_id': 'User Name'},
	         'width': '80px'},
	        {'if': {'column_id': 'Followers'},
	        'textAlign': 'center',
	         'width': '40px'},
	        {'if': {'column_id': 'Tweet'},
	         'width': '450px'},
	        {'if': {'column_id': 'Retweets Selected'},
	        'textAlign': 'center',
	         'width': '40px'},
	        {'if': {'column_id': 'Retweets Total'},
	        'textAlign': 'center',
	         'width': '40px'},
	        {'if': {'column_id': 'Tags'},
	         'width': '80px'},
	        {'if': {'column_id': 'URL'},
	         'width': '80px'},   
    	]

rule_toptweets = '''
            max-height: 300px; min-height: 30px; height: 150px;
            display: block;
            overflow-y: hidden;
       		'''

rule_topusers = '''
            max-height: 300px; min-height: 30px; height: 100px;
            display: block;
            overflow-y: hidden;
       		'''

def clean_top_users(top_users):
	df = (top_users[['user_name','followers_count','retweeted','user_description']]
	  ).rename(columns={
	  'user_name':'User Name', 
	  'followers_count':'Followers',
	  'retweeted':'Retweets',
	  'user_description':'User Description'})

	df['User Name'] = df['User Name'].apply(lambda x: 
		'[' + str(x) + '](https://twitter.com/'+  str(x) + ')'
	)
	df['User Description'] = df['User Description'].apply(lambda x: 
		str(x).replace('"','').replace("'",'')
		)

	return df

col_widths_topusers = [
	        {'if': {'column_id': 'User Name'},
	         'width': '120px'},
	        {'if': {'column_id': 'Followers'},
	         'textAlign': 'center',
	         'width': '40px'},
	        {'if': {'column_id': 'Retweets'},
	        'textAlign': 'center',
	         'width': '40px'},
	        {'if': {'column_id': 'User Description'},
	         'width': '450px'},
	         ]


no_data_txt = '<center>No matching data found.</center>' 
no_data_tbl = html.Iframe(srcDoc=no_data_txt, sandbox='',
		style={'height': '150px', 'width':'75%', 
			'border-radius': '5px',
			'display': 'block',
			'margin-left': 'auto',
			'margin-right': 'auto',
			'padding-top': '125px',
			'color': '#23272a',
    		'background-color': '#7289DA21',
    		'border-style': 'hidden',})


def gen_dash_table(id, df_subsets, subset='now_1h', type='top_tweets'):

	df = df_subsets[df_subsets.subset==subset]
	if len(df)==0: return no_data_tbl

	if type=='top_tweets':
		#if isnan(df.RT_id.iloc[0]): return None
		#if df.RT_id.iloc[0]=='': return None
		if df.RT_id.iloc[0] in ['nan', '']: return no_data_tbl
		df = clean_top_tweets(df)
		col_widths = col_widths_toptweets
		rule = rule_toptweets
	else:
		#if isnan(df.user_id.iloc[0]): return None
		if df.user_id.iloc[0] in ['nan', '']: return no_data_tbl
		df = clean_top_users(df)
		col_widths = col_widths_topusers 
		rule = rule_topusers

	#print(df)

	if type=='top_tweets': 
		columns = [{'id': c, 'name': c} for c in df.columns.drop(['URL','User Name'])]
		columns.append({'name': 'URL', 'id':'URL','type':'text','presentation':'markdown'})
		columns.insert(0, {'name': 'User Name', 'id':'User Name','type':'text','presentation':'markdown'})
	elif type=='top_users': 
		columns = [{'id': c, 'name': c} for c in df.columns.drop('User Name')]
		columns.insert(0, {'name': 'User Name', 'id':'User Name','type':'text','presentation':'markdown'})
	else:
		columns = [{'id': c, 'name': c} for c in df.columns]

	return dash_table.DataTable(
		id = id,
	    data=df.to_dict('records'),
	    columns=columns,
	    tooltip_data=[
	        {
	            column: {'value': str(value), 'type': 'markdown'}
	            for column, value in row.items()
	        } for row in df.to_dict('rows')
	    ],
	    tooltip_duration=None,
	    style_cell={
		    'whiteSpace': 'normal',
	        'height': 'auto',
	        'textAlign': 'left',
	        'lineHeight': '25px',
	        'font_size': '16px',
	        'backgroundColor': 'rgb(50, 50, 50)',
        	'color': 'white',
        	#'textOverflow': 'ellipsis',
        	#'minWidth': '40px', 'width': '300px', 'maxWidth': '500px',
        },
        style_as_list_view=True,
        style_header={
        	'fontWeight': 'bold',
        	'backgroundColor': 'rgb(30, 30, 30)'
    	},
	    style_cell_conditional=col_widths,
    	style_table={'overflowX': 'auto'},
    	# style_table={'minWidth': '100%'},
    	css=[{
        'selector': '.dash-spreadsheet td div',
        'rule': rule
   		 }],
	    )


def fig_top_users(top_users, subset='now_1h'):
	#import plotly.graph_objects as go
	df = top_users[top_users.subset==subset]
	if len(df)==0: return no_data_fig
	#if isnan(df.user_id.iloc[0]): return go.Figure()
	if df.user_id.iloc[0] in ['nan', '']: return no_data_fig
	
	user = df.user_name
	retweeted = df.retweeted
	followers = df.followers_count
	colors = ['#ff5733', '#33dbff']

	fig = go.Figure(
	    data=[
	        go.Bar(x=user, y=retweeted, name="Retweeted",
	           marker_color=colors[0],yaxis='y',
	           offsetgroup=1),
	        go.Bar(x=user, y=followers, name="Followers", 
	           marker_color=colors[1],yaxis='y2',
	           offsetgroup=2)
	    ],
	    layout={
	        'yaxis': {'title': '<b>Retweeted during the time span</b>',
	                 'color': colors[0], 'showgrid': True, 
	                  'gridwidth': 1, 'gridcolor': '#ffbbb8'},
	        'yaxis2': {'title': '<b>Followers</b>', 'overlaying': 'y', 'side': 'right',
	                  'color': colors[1], 'showgrid': True, 
	                  'gridwidth': 1, 'gridcolor': '#b8fffa'}
	    }
	)
	    
	fig.update_layout(
	    title_text='<b>Top Influencers</span></b>',
	    plot_bgcolor='white',
	    showlegend=False,
	    barmode='group',
	    xaxis_tickangle=-45
	)

	#fig.show()
	return fig


  
    


