import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import pandas as pd

from helpers import *

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

df_example_sentiments = pd.read_csv('df_example_sentiments.csv')
df_example_emotions = pd.read_csv('df_example_emotions.csv')


app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


colors = {
'background': '#23272a',
'text': '#7289da'
}



app.layout = html.Div(
	style={'backgroundColor': colors['background'],
	'maxWidth': '960px', 'margin': 'auto'},
	children = [
	    html.H1(
			"Tweets for #BlackLivesMatter",
			style = {'textAlign': 'center', 'color': colors['text']}
		),
		html.H5(			
			"Track the BLM movement with Twitter Data. ",
			style = {'textAlign': 'center', 'color': colors['text']}
		),
		dcc.Tabs(id='tabs', value='tab-sentiments', 
			children=[
	        	dcc.Tab(label='Sentiments', value='tab-sentiments'),
	        	dcc.Tab(label='Top Words', value='tab-topwords'),
	        	dcc.Tab(label='Top Tweets', value='tab-toptweets'),
	        	dcc.Tab(label='About', value='tab-about')
    ]),
    html.Div(id='tabs-content')
])




@app.callback(Output('tabs-content', 'children'),
              [Input('tabs', 'value')])
def render_content(tab):
    if tab == 'tab-sentiments': 
    	return html.Div([
			dcc.Graph(
				figure = fig_sentiments(df_example_sentiments)
			),
			dcc.Graph(
				figure = fig_emotions(df_example_emotions)
			)
			])
    elif tab == 'tab-topwords': 
    	return html.Div([
            html.H3('Tab content top words')
        ])
    elif tab == 'tab-toptweets': 
    	return html.Div([
            html.H3('Tab content top tweets')
        ])
    elif tab == 'tab-about': 
    	return  html.Div([
            html.H3('Tab content About')
        ])


if __name__ == '__main__':
	app.run_server(debug=True)
