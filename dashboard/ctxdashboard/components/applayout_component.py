from dataclasses import dataclass
from dash import html, dcc
import dash_bootstrap_components as dbc
import pandas as pd
from ctxdashboard.components.filterform_component import FilterFormComponent


@dataclass
class AppLayoutComponent:
    body: html.Div
    out_piechart: html.Div
    out_acceptance_heatmap: html.Div
    out_times_heatmap: html.Div
    filter_form: FilterFormComponent

    @classmethod
    def createComponent(cls, patient_cofactors: pd.DataFrame) -> "AppLayoutComponent":
        pie_holder = html.Div(
            className="pie-holder",
            id="pie-holder"
        )

        patient_heatmap_acceptance = html.Div(
            className="pat-heatmap-times"
        )

        patient_heatmap_times = html.Div(
            className="pat-heatmap-times"
        )

        filter_form_component = FilterFormComponent.createComponent(
            patient_cofactors)

        right_children = [
            pie_holder,
            html.Div(
                className="patient-heatmap-wrapper",
                children=[patient_heatmap_acceptance,
                          patient_heatmap_times]
            )
        ]

        app_layout = html.Div(
            className="container",
            children=[
                html.Div([
                    html.Img(src='/assets/meduni-logo.png'),
                    html.H1(children='CTx Activity Tracker', className="page-header"),
                ], className="header-wrapper"),
                dbc.Row(
                    className="page",
                    children=[
                        html.Div(
                            className="left",
                            children=filter_form_component.body
                        ),
                        html.Div(
                            className="right",
                            children=html.Div(
                                className="right-inner",
                                children=dcc.Loading(
                                    right_children
                                )
                            )
                        )
                    ]
                )]
        )
        return cls(app_layout,
                   pie_holder,
                   patient_heatmap_acceptance,
                   patient_heatmap_times,
                   filter_form_component)
