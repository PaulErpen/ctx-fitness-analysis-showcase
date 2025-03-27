import os
from typing import Dict, List, Tuple
from ctxdashboard.components.applayout_component import AppLayoutComponent
from dash import Dash, html, dcc, Output, Input
import pandas as pd
from ctxfitness.preprocessing_pipeline import ParsedDailiesColumns as pdc
from ctxfitness.ctx_patient_dailies_handler import CTxPatientDailiesHandler, simple_min_hours_daily_acceptance_criterion, minimum_overall_and_consecutive_days_patient_acceptance_criterion
import dash_bootstrap_components as dbc
import dash_auth
from ctxdashboard.figures.patient_heatmap import render_acceptance_heatmap, render_times_heatmap
from ctxdashboard.figures.pie_chart import create_acceptance_pie_chart
from ctxdashboard.filter_patients.filter_patients import filter_patients

normalized_dailies: pd.DataFrame = pd.read_excel(
    os.path.join(os.path.dirname(__file__), "../data/dailies.xlsx")
).sort_values(pdc.USER_LAST_NAME, ascending=False)

patient_cofactors: pd.DataFrame = pd.read_excel(
    os.path.join(os.path.dirname(__file__), "../data/patient-meta.xlsx"))

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

layout = AppLayoutComponent.createComponent(patient_cofactors)

app.layout = layout.body


@ app.callback(
    Output(layout.out_times_heatmap, component_property='children'),
    Output(layout.out_acceptance_heatmap, component_property='children'),
    Output(layout.filter_form.out_min_hours_per_day_display,
           component_property='children'),
    Output(layout.out_piechart, component_property='children'),
    Input(layout.filter_form.in_min_hours_per_day_input,
          component_property='value'),
    Input(layout.filter_form.in_min_days_input, component_property="value"),
    Input(layout.filter_form.in_min_consecutive_days_input, component_property="value"),
    Input(layout.filter_form.in_ecog_multi_select, component_property="value"),
    Input(layout.filter_form.in_age_slider, component_property="value"),
    Input(layout.filter_form.in_gender_multi_select, component_property="value"),
    Input(layout.filter_form.in_therapy_multi_select,
          component_property="value"),
    Input(layout.filter_form.in_therapy_regimen_multi_select,
          component_property="value"),
    Input(layout.filter_form.in_treatment_naive_multi_select,
          component_property="value"),
    Input(layout.filter_form.in_prior_treatment_multi_select,
          component_property="value"),
    Input(layout.filter_form.in_primary_tumor_multi_select,
          component_property="value"),
    Input(layout.filter_form.in_tug_range_slider, component_property="value"),
    Input(layout.filter_form.in_tug_nan_checkbox, component_property="value"),
    Input(layout.filter_form.in_hgs_range_slider, component_property="value"),
    Input(layout.filter_form.in_hgs_nan_checkbox, component_property="value"),
)
def update_output_div(
    min_hours_per_day: int,
    min_days_input: int,
    min_consecutive_days_input: int,
    ecog_values: List[str],
    age_interval: Tuple[str, str],
    gender_multi_select_values: List[str],
    therapy_multi_select: List[str],
    therapy_regimen_multi_select: List[str],
    treatment_naive_multi_select: List[str],
    prior_treatment_multi_select: List[str],
    primary_tumor_multi_select: List[str],
    tug_range_slider: Tuple[str, str],
    tug_nan_checkbox: List[str],
    hgs_range_slider: Tuple[str, str],
    hgs_nan_checkbox: List[str],
):
    filtered_ids = filter_patients(patient_cofactors=patient_cofactors,
                                   ecog_values=ecog_values,
                                   age_interval=age_interval,
                                   gender_multi_select_values=gender_multi_select_values,
                                   therapy_multi_select=therapy_multi_select,
                                   therapy_regimen_multi_select=therapy_regimen_multi_select,
                                   treatment_naive_multi_select=treatment_naive_multi_select,
                                   prior_treatment_multi_select=prior_treatment_multi_select,
                                   primary_tumor_multi_select=primary_tumor_multi_select,
                                   tug_range_slider=tug_range_slider,
                                   tug_include_nans=tug_nan_checkbox,
                                   hgs_range_slider=hgs_range_slider,
                                   hgs_include_nans=hgs_nan_checkbox)

    patient_dailies_handlers: List[CTxPatientDailiesHandler] = []
    total_durations_sorted = normalized_dailies.groupby(pdc.USER_LAST_NAME)[pdc.DAILY_DURATION_S].sum().sort_values(ascending=True)
    for user_id in total_durations_sorted.index:
        if str(user_id) in filtered_ids:
            sub_frame = normalized_dailies[normalized_dailies[pdc.USER_LAST_NAME] == user_id]
            if sub_frame.shape[0] > 0:
                patient_dailies_handlers.append(CTxPatientDailiesHandler.from_frame(
                    user_id,
                    sub_frame,
                    simple_min_hours_daily_acceptance_criterion(min_hours_per_day),
                    minimum_overall_and_consecutive_days_patient_acceptance_criterion(min_days_input, min_consecutive_days_input)))
    heatmap_graph = dcc.Graph(
        className="pat-heatmap-times-svg",
        figure=render_times_heatmap(
            patient_dailies_handlers, min_hours_per_day),
        config=dict(
            displayModeBar=False
        ),
        responsive=True
    ) if len(patient_dailies_handlers) > 0 else html.Div()

    heatmap_acceptance_graph = dcc.Graph(
        className="pat-heatmap-acceptance-svg",
        figure=render_acceptance_heatmap(patient_dailies_handlers),
        config=dict(
            displayModeBar=False
        ),
        responsive=True
    ) if len(patient_dailies_handlers) > 0 else html.Div()

    pie_chart = (create_acceptance_pie_chart([d.accepted for d in patient_dailies_handlers], patient_cofactors.shape[0])
                 if len(patient_dailies_handlers) > 0
                 else html.Div("No patients match this selection...", className="warning"))

    return (
        heatmap_graph,
        heatmap_acceptance_graph,
        f"Minimum of {int(min_hours_per_day)} hours per day:",
        pie_chart
    )


server = app.server

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0",
                   port=int(os.environ.get("PORT", 8080)))
