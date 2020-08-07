import requests
import plotly.graph_objects as go


def count_words_at_url(url):
    resp = requests.get(url)
    return [len(resp.text.split()), 'abc']


def city_extended(city, version):
	if city=='all':
		city_extended = city + '_v' + str(version) 
		return [city_extended, {'min-width': '100px', 'display': 'flex'}]
	else:
		return [city, {'display': 'none'}]


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
    return fig.to_json()

