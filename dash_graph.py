from dash import dash, dcc, html, Input, Output, dash_table
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
import pandas as pd

def main(data):
    external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

    app = dash.Dash(external_stylesheets=external_stylesheets, )
    wells = []
    wells = data.well_name.unique()

    app.layout = html.Div([
        html.H4(children=""),
        dcc.Tabs(
            id='tabs', value='graph', children=[
                dcc.Tab(label='tm data', value='ADKU'),
            ]
        ),
        html.Div(id='tabs_content')
    ])

    @app.callback(
        Output('tabs_content', 'children'),
        Input('tabs', 'value'),
    )
    def render_tabs(tab):
        if tab == 'ADKU':
            return html.Div([
                dcc.Dropdown(wells, id='well',
                             style={'width': '40%', 'display': 'inline-block'}),
                html.H4(children='Clickhouse data'),
                dcc.Graph(id='tm_data'),
            ])

    @app.callback(
        Output('tm_data', 'figure'),
        Input('well', 'value'),
    )

    def get_graph_by_well(well):
        df_w = data[data.well_name == well]
        params = sorted(df_w.param_id.unique())
        # params = [1, 2, 3, 8, 35, 41, 46]
        tm_fig = make_subplots()
        try:
            df_w = data[data.well_name == well]
            for param in params:
                df_p = df_w[df_w.param_id == param]

                try:
                    param_name = df_p.descriptions.iloc[0]
                    p_name = str(param) + ' ' + param_name
                except:
                    p_name = str(param)
                tm_fig.add_trace(
                    go.Scattergl(x=df_p.dt.tolist(), y=df_p.val.tolist(), name=p_name, mode='lines+markers'
                                 ))
        except:
            print('error')

        return tm_fig

    app.run_server(debug=False)
