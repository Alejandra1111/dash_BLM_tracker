import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from plotly.colors import n_colors
from collections import Counter
from wordcloud import WordCloud, ImageColorGenerator
import matplotlib.pyplot as plt
from PIL import Image
import dash_table
import copy
import itertools 
from io import BytesIO
import base64
#from glob import glob 
from math import isnan
import dash_html_components as html



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
    #print(input_container[pos])
    if 'props' in input_container[pos]:
        input_container[pos]['props']['value'] = ''
    elif hasattr(input_container[pos],'value'):
        input_container[pos].value = ''


def set_task_in_progress_false(input_container, pos, children=[False]):
    #print(input_container[pos])
    if 'props' in input_container[pos]:
        input_container[pos]['props']['children'] = children
    elif hasattr(input_container[pos],'children'):
        input_container[pos].children = children


def set_picked_version_style(input_container, pos1, pos2):
    #print(input_container[pos1], input_container[pos2])
    if 'props' in input_container[pos1]:
        city = input_container[pos1]['props']['value']
        #print(city, input_container[pos2]['props']['style']) 
        if city=='all': 
            input_container[pos2]['props']['style'] = {'min-width': '100px', 'display': 'flex'}
        else:
            input_container[pos2]['props']['style'] = {'display': 'none'}
    elif hasattr(input_container[pos1],'value'):
        city = input_container[pos1].value 
        #print(city, input_container[pos2].style) 
        if city=='all': 
            input_container[pos2].style = {'min-width': '100px', 'display': 'flex'}
        else:
            input_container[pos2].style = {'display': 'none'}


def stat_list(stats):
    return [stats['stat_sentiments'], stats['stat_emotions'], stats['stat_words'], 
        stats['top_tweets'], stats['top_users'], stats['type']]


def wAvg(df, var_stat ='mean', var_count='count', digits=3):
    if len(df)==0: return None
    df.loc[:,'__tmp1'] = df[var_count]/sum(df[var_count])
    df.loc[:,'__tmp2'] = df[var_stat] * df['__tmp1'] 
    return df['__tmp2'].agg('sum').round(digits)


def formatInt(df, varnames):
    for varname in varnames:
        df[varname] = df[varname].apply(lambda x: f"{x:,}")


def fix_datetime(df, timevar='created_at_h'):
    df[timevar] = pd.to_datetime(df[timevar])

def fix_token_counter(df):
    df.token_counter = df.token_counter.apply(lambda x: Counter(x))  

def fix_RT_id(df):
    df.RT_id = df.RT_id.astype(str)

def fix_user_id(df):
    df.user_id = df.user_id.astype(str)    




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
    # x_data_max = pd.to_datetime(x_data).max()
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
        # rangeslider_visible=True,
        # rangeslider_range= [x_data_max - pd.Timedelta(7, unit='d'), x_data_max],
        # rangeslider=dict(
        #     visible=True, 
        #     range =  [x_data_max - pd.Timedelta(7, unit='d'), x_data_max]
        #     ),
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
        ),
    )

    fig.update_layout(
        title_text='<b>Emotions</span></b> in Tweets',
        plot_bgcolor='white',
        legend_orientation="h",
        legend=dict(
            font_size=14,
            x = 0,
            y = 1.02,
            
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


def fig_dashboard(stats, time):
    if len(stats)==0: return no_data_fig

    fct_resize = .85

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x= stats.today,
        y= stats.city,
        marker=dict(color="#ff8e25", size=18 * fct_resize),
        mode="markers",
        name="Today",
    ))

    fig.add_trace(go.Scatter(
        x= stats.yesterday,
        y= stats.city,
        marker=dict(color="#f6e925", size=14 * fct_resize),
        mode="markers",
        name="Yesterday",
    ))

    fig.add_trace(go.Scatter(
        x= stats.seven_days,
        y= stats.city,
        marker=dict(color="#6ede1f", size=14 * fct_resize),
        mode="markers",
        name="Past 7 days",
    ))

    fig.add_trace(go.Scatter(
        x= stats.two_weeks,
        y= stats.city,
        marker=dict(color="#2593ff", size=14 * fct_resize),
        mode="markers",
        name="Past two weeks",
    ))


    fig.update_layout(title="Sentiments ",
                      xaxis_title="",
                      yaxis_title="",
                        width=800 * fct_resize,
                        height=800 * fct_resize,
                        margin=dict(l=40, r=40, b=40, t=40),
                        font=dict(size=15 * fct_resize),
                      legend=dict(
                        orientation="h",
                        font_size=14 * fct_resize,
                        yanchor="bottom",
                        y=1.01,
                        xanchor="right",
                        x=1
                        ),
                        plot_bgcolor='white',
                     )
    fig.update_xaxes(showline=False, #linewidth=2, linecolor= '#D3D3D3', 
                     showgrid=True, gridwidth=1, gridcolor= '#D3D3D3',
                     zeroline=True, zerolinewidth=2, zerolinecolor='#D3D3D3')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor= 'slategray')

    annotations = []
    annotations.append(
        dict(xref='paper', yref='paper',
            x= -0.3, 
            xanchor= 'left',
            y= 1, 
            yanchor= 'bottom',
            showarrow=False,
            text= '({time} CST)'.format(time=str(time))))

    annotations.append(
        dict(xref='paper', yref='paper',
            x= 1.05, 
            xanchor= 'left',
            y= 0.95, 
            yanchor= 'bottom',
            showarrow=False,
            text= 'Comparisons<br> (mouseover for details)'))

    colors = np.array(n_colors('rgb(200, 0, 0)', 'rgb(255, 200, 200)', 5, colortype='rgb') +
                     ['rgb(200,200,200)'] +
                     n_colors('rgb(200, 200, 255)', 'rgb(0, 0, 200)', 5, colortype='rgb'))

    bins = [-1, -0.8, -0.6, -0.4, -0.2, 0.2, 0.4, 0.6, 0.8, 1]

    label_names = ['Difference from All Cities', 'Change from yesterday',
                  'Change from past 7 days', 'Change from past two weeks']  
    label_x = [x*0.125 + 1.15 for x in range(4)] 

    def add_plus(x):
        return '+' + str(x) if x>=0.01 else str(x)

    # add labels on the right_side of the plot
    for x, labels, name in zip(label_x, 
                         [stats.diff_from_all, stats.ch_from_yest, 
                          stats.ch_from_sevend, stats.ch_from_twoweek],
                        label_names):
        
        lab_colors = list(colors[np.digitize(labels, bins)])
        
        for y, label, color in zip( stats.city, labels, lab_colors):

            annotations.append(
                dict(xref='paper', x=x, y=y,
                      xanchor='right', yanchor='middle',
                      text=add_plus(label),
                      hovertext = y + '<br>' + name + ': '+ add_plus(label),
                      font=dict(size=16 * fct_resize,
                                 color = color),            
                      showarrow=False)
            )

               
    fig.update_layout(
        annotations = annotations,
        #autosize=False,
        margin=dict(
            #autoexpand=False,
            r=230 * fct_resize,
        )
        )     
    #fig.show()
    return fig 


    



