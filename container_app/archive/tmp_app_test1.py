# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import time

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


colors = {
    'background': '#111111',
    'text': '#7FDBFF'
}

app.layout = html.Div(style={'backgroundColor': colors['background']}, children=[
    html.H1(
        children='Hello Dash',
        style={
            'textAlign': 'center',
            'color': colors['text']
        }
    ),

    html.Div(children='Dash: A web application framework for Python.', style={
        'textAlign': 'center',
        'color': colors['text']
    }),

    dcc.Graph(
        id='example-graph-2',
        figure={
            'data': [
                {'x': [1, 2, 3], 'y': [4, 1, 2], 'type': 'bar', 'name': 'SF'},
                {'x': [1, 2, 3], 'y': [2, 4, 5], 'type': 'bar', 'name': u'Montréal'},
            ],
            'layout': {
                'plot_bgcolor': colors['background'],
                'paper_bgcolor': colors['background'],
                'font': {
                    'color': colors['text']
                }
            }
        }
    ),
    html.Button('Button 1', id='button1'),
    html.Div(id='wrapper2', children=html.Button('Button 2', id='button2')),
    html.Button('Button 3', id='button3', style=dict(display='none')),
    html.Div(id='updatingbtn'),
    html.Div(id='updatingbtn2'),
])


@app.callback(
    Output('button1', 'style'),
    [Input('button1', 'n_clicks')]
    )
def buttonevent(n_clicks):
    if n_clicks is None: return None
    if n_clicks>0:
        print('Button 1 pressed!')
        return {'display': 'none'}
    else:
        return None 

@app.callback(
    Output('updatingbtn','children'),
    [Input('button2','n_clicks')])
def update1(n_clicks):
    if n_clicks is None: return None
    if n_clicks>0:
        print('Button 2 pressed!')
        time.sleep(3)
        
        return gen_callback1('buttonx')
    else: return None


def gen_callback1(btnid):
    print('in gen_callback1:')

    @app.callback(
        Output(btnid, 'style'),
        [Input(btnid,'n_clicks')]
        )
    def do_something(n_clicks):
        print('doing something')
        if n_clicks is None: return dict(display='inline')
        if n_clicks>0:
            print('Button ' + btnid + 'pressed!')
            return dict(display='none')
        else: None
    return [html.Button('Button X', id=btnid)]


@app.callback(
    Output('button2', 'style'),
    [Input('button2','n_clicks')]
    )
def update1(n_clicks):
    if n_clicks is None: return dict(display='inline')
    if n_clicks>0:
        print('Button 2 hidden!')
        return {'display': 'none'}
    else: return dict(display='inline')



# @app.callback(
#     [Output('button2', 'style'),
#     Output('button3','style')],
#     [Input('button2','n_clicks'),
#      Input('button3','n_clicks')]
#     )
# def update1(n_clicks2,n_clicks3):
#     if n_clicks2 is None: return dict(display='None')
#     [print(item['prop_id']) for item in dash.callback_context.triggered]
#     context = dash.callback_context.triggered[0]['prop_id'].split('.')[0]
#     print(context)

#     if (n_clicks2>0) & (context=='button2'):
#         print('Button 2 pressed!')
#         time.sleep(5)
#         return dict(display='inline')

#     else: return dict(display='None')




# @app.callback(
#     Output('button2','style'),
#     [Input('button3','n_clicks')]
#     )
# def update1(n_clicks):
#     if n_clicks is None: return dict(display='None')
#     if n_clicks>0:
#         print('Button 3 pressed!')
#         time.sleep(5)
#         return dict(display='inline')
#     else: return dict(display='None')

# @app.callback(
#     Output('button3', 'style'),
#     [Input('button3','n_clicks')]
#     )
# def update1(n_clicks):
#     if n_clicks is None: return dict(display='inline')
#     if n_clicks>0:
#         print('Button 3 hidden!')
#         return {'display': 'none'}
#     else: return dict(display='inline')






if __name__ == '__main__':
    app.run_server(debug=True)