import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from app import app
import pandas as pd
import dash_table
import src.helper as h


dates = ['2018-2-2', '2018-2-3', '2018-2-4', '2018-2-5', '2018-2-6', '2018-2-7', '2018-2-8', '2018-2-9',
        '2018-3-2', '2018-3-3', '2018-3-4', '2018-3-5', '2018-3-6', '2018-3-7', '2018-3-8', '2018-3-9']

layout = html.Div([
    html.H5('News / Tweet Dashboard', style={'margin-bottom': '5px', 'borderBottom': 'thin lightgrey solid'}),

    html.Div([
        html.P('Search for news articles by keyword', className='alabel'),
        dcc.Input(
            id='input-keyword',
            placeholder='Enter a keyword...',
            type='text',
            value='',
            style={'width': '150px', 'height': '30px'}
        )
    ], style={'margin-bottom': '10px'}),

    html.Div([
        html.P('Select a day', className='alabel'),
            dcc.Dropdown(
                    id='dropdown-day',
                    options=[{'label': i, 'value': i}
                        for i in dates],
                    value='',
                    style={'width': '150px'}
            ),
    ], style={'margin-bottom': '10px'}),
    

    html.Button('Run', id='button', className='gen-button'),

    html.Div(id='news-content'),

    html.Div(id='blah2', style={'display': 'none'}), 
    html.Div(dash_table.DataTable(id='table', data=[{}]), style={'display': 'none'}),
    html.Div(style={'margin-top': '200px'})

])


@app.callback(
    Output('news-content', 'children'),
    [Input('button', 'n_clicks')],
    [State('input-keyword', 'value'),
     State('dropdown-day', 'value')])
def get_news(n_clicks, keyword, day):
        if len(keyword) == 0:
            return

        df = h.get_top_news(keyword, day)
        df['text'] = df['text'].apply(lambda x: x[:100] + '..')

        return dash_table.DataTable(
            id='table',
            columns=[{"name": i, "id": i} for i in df.columns],
            data=df.to_dict("rows"),
            row_selectable="single",
            editable=False,
            style_header={
                'backgroundColor': 'rgb(248, 248, 248)',
                'fontSize': '14px'
            },
            style_cell={
                'minWidth': '0px', 'maxWidth': '180px',
                'whiteSpace': 'normal'
            },
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
            }],
        )

@app.callback(
    Output('blah2', 'children'),
    [Input('table', 'data')])
def dummy_update(data):
    print('inside dummy update')
    # for open issue with data table
    return html.Div()