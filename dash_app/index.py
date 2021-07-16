import dash_core_components as dcc
import dash_html_components as html

from dash.dependencies import Input, Output

from src import apph
from app import app

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

# Loading screen CSS
app.css.append_css({
    "external_url": "https://codepen.io/chriddyp/pen/brPBPO.css"})


@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    return apph.layout

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', debug=True)