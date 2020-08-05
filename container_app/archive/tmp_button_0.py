import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State, MATCH, ALL
import time

app = dash.Dash(__name__, suppress_callback_exceptions=True)

print('App is starting..')

app.layout = html.Div([
    html.Div(id='dynamic-button-container', 
    	children=[
    	html.Button(
    		id={'type': 'dynamic-button', 'index': 0 },
    		children= 'Button'
    		)
    	]),
])

@app.callback(
    Output('dynamic-button-container', 'children'),
    [Input({'type': 'dynamic-button', 'index': ALL}, 'n_clicks')],
    [State('dynamic-button-container', 'children')])
def display_newbutton(n_clicks, children):
	if n_clicks[0] is None: return children 
	else:
		print('Doing some calculation..') 
		time.sleep(3)

		new_element = html.Button(
		        id={'type': 'dynamic-button','index': 0 }, #n_clicks[0] },
		        children = 'Button' 
		    	)

		children.pop()
		children.append(new_element)
		print('Generating a new button')
		return children

@app.callback(
    Output({'type': 'dynamic-button', 'index': MATCH}, 'disabled'),
    [Input({'type': 'dynamic-button', 'index': MATCH}, 'n_clicks')]
)
def hide_newbutton(n_clicks):
	if n_clicks is None: return False
	else:
		print('Disabling the button')
		return True


# @app.callback(
#     Output('dynamic-button-container', 'children'),
#     [Input('button0', 'n_clicks')],
#     [State('dynamic-button-container', 'children')])
# def display_newbutton(n_clicks, children):
# 	if n_clicks>0:
# 		print('Generating a new button: Button ' + str(n_clicks))

# 		new_element = html.Button(
# 		        id={
# 		            'type': 'dynamic-button',
# 		            'index': n_clicks
# 		        },
# 		        children = 'Button ' + str(n_clicks)
# 		    	)
# 		time.sleep(3)
# 		#children.pop()
# 		children.append(new_element)
# 		print(children)
# 	return children



# @app.callback(
#     Output({'type': 'dynamic-button', 'index': MATCH}, 'style'),
#     [Input({'type': 'dynamic-button', 'index': MATCH}, 'n_clicks')]
# )
# def hide_newbutton(n_clicks):
# 	if n_clicks>0:
# 		print('Hiding Button' + str(n_clicks))
# 		return dict(display='none') 
# 	else: return None






if __name__ == '__main__':
    app.run_server(debug=True)


