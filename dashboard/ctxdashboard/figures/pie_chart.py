from typing import List
import plotly.graph_objects as go
from dash import dcc

def create_acceptance_pie_chart(acceptances: List[bool], n_total_patients):
    n_patients_after_filters = len(acceptances)
    n_accept = sum(acceptances)
    figure = go.Figure(
        data=go.Pie(
            labels=["accepted", "not accepted", "dropped because of filters"],
            values=[
                n_accept,
                n_patients_after_filters - n_accept,
                n_total_patients - n_patients_after_filters
            ],
            marker={
                "colors": ["green", "red", "#b8b8b8"]
            }
        )
    )
    figure.update_traces(textinfo='value+percent')
    return dcc.Graph(
        id=f"acceptance-pie-figure",
        className="acceptance-pie-figure",
        figure=figure,
        config={
            'displayModeBar': False
        }
    )
