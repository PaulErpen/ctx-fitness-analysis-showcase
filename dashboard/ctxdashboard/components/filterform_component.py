from dataclasses import dataclass
from typing import Tuple
from dash import html, dcc
from ctxdashboard.components.series_dropdown import get_series_dropdown, InitialSelection, LabelingStrategy
import pandas as pd
from ctxdashboard.domain.patientmeta import PatientMetaColumn as pmc
from math import ceil, floor


def interval_slider(id: str, min_value: int, max_value: int) -> dcc.RangeSlider:
    return dcc.RangeSlider(
        min_value,
        max_value,
        step=1,
        value=[min_value, max_value],
        marks=None,
        tooltip={"placement": "bottom", "always_visible": True},
        id=id)


def get_min_max(series: pd.Series) -> Tuple[int, int]:
    min_value = floor(series.min())
    max_value = ceil(series.max())
    return (min_value, max_value)


@dataclass
class FilterFormComponent:
    body: html.Div
    out_min_hours_per_day_display: html.Div
    in_min_days_input: dcc.Input
    in_min_consecutive_days_input: dcc.Input
    in_ecog_multi_select: dcc.Dropdown
    in_min_hours_per_day_input: dcc.Slider
    in_age_slider: dcc.RangeSlider
    in_gender_multi_select: dcc.Dropdown
    in_therapy_multi_select: dcc.Dropdown
    in_therapy_regimen_multi_select: dcc.Dropdown
    in_treatment_naive_multi_select: dcc.Dropdown
    in_prior_treatment_multi_select: dcc.Dropdown
    in_primary_tumor_multi_select: dcc.Dropdown
    in_tug_range_slider: dcc.RangeSlider
    in_tug_nan_checkbox: dcc.Checklist
    in_hgs_range_slider: dcc.RangeSlider
    in_hgs_nan_checkbox: dcc.Checklist

    @classmethod
    def createComponent(cls, patient_cofactors: pd.DataFrame) -> "FilterFormComponent":
        min_hours_per_day_display = html.Div(
            children="",
            className="field-label"
        )

        min_days_input = dcc.Input(
            id="min-number-days-input",
            type="number",
            value=6
        )

        min_consecutive_days_input = dcc.Input(
            id="min-number-consecutive-days-input",
            type="number",
            value=6
        )

        ecog_multi_select = get_series_dropdown(
            "ecog",
            patient_cofactors[pmc.ECOG],
            initial_selection=InitialSelection.NONE,
            labeling_strategy=LabelingStrategy.AS_INTEGER,
            multi=True
        )

        min_hours_per_day_input = dcc.Slider(
            1,
            24,
            step=1,
            value=8,
            id='year-slider',
            marks=None,
            tooltip={"placement": "bottom", "always_visible": True}
        )

        gender_multi_select = get_series_dropdown(
            "gender",
            patient_cofactors[pmc.GENDER],
            initial_selection=InitialSelection.NONE,
            labeling_strategy=LabelingStrategy.AS_STRING,
            multi=True
        )

        min_age, max_age = get_min_max(patient_cofactors[pmc.AGE])
        age_slider = interval_slider("age", min_age, max_age)

        therapy_multi_select = get_series_dropdown(
            "therapy",
            patient_cofactors[pmc.THERAPY],
            initial_selection=InitialSelection.NONE,
            labeling_strategy=LabelingStrategy.AS_STRING,
            multi=True
        )

        therapy_regimen_multi_select = get_series_dropdown(
            "therapy-regimen",
            patient_cofactors[pmc.THERAPY_REGIMEN],
            initial_selection=InitialSelection.NONE,
            labeling_strategy=LabelingStrategy.AS_STRING,
            multi=True
        )

        treatment_naive_multi_select = get_series_dropdown(
            "treatment-naive",
            patient_cofactors[pmc.TREATMENT_NAIVE],
            initial_selection=InitialSelection.NONE,
            labeling_strategy=LabelingStrategy.AS_STRING,
            multi=True
        )

        prior_treatment_multi_select = get_series_dropdown(
            "prior-treatment",
            patient_cofactors[pmc.PRIOR_TREATMENT],
            initial_selection=InitialSelection.NONE,
            labeling_strategy=LabelingStrategy.AS_STRING,
            multi=True
        )

        primary_tumor_multi_select = get_series_dropdown(
            "primary-tumor",
            patient_cofactors[pmc.PRIMARY_TUMOR],
            initial_selection=InitialSelection.NONE,
            labeling_strategy=LabelingStrategy.AS_STRING,
            multi=True
        )

        min_tug, max_tug = get_min_max(patient_cofactors[pmc.TUG])
        tug_range_slider = interval_slider("tug", min_tug, max_tug)
        tug_nan_checkbox = dcc.Checklist(
            options=["Include unknown"],
            value=["Include unknown"],
        )

        min_hgs, max_hgs = get_min_max(patient_cofactors[pmc.HGS])
        hgs_range_slider = interval_slider("hgs", min_hgs, max_hgs)
        hgs_nan_checkbox = dcc.Checklist(
            options=["Include unknown"],
            value=["Include unknown"],
        )

        body = html.Div(
            className="filter-form",
            children=[
                html.H3("Acceptance criteria"),
                min_hours_per_day_display,
                min_hours_per_day_input,
                html.Div(
                    children="Minimum number of days:",
                    className="field-label"
                ),
                min_days_input,
                html.Div(
                    children="Minimum number of consecutive days:",
                    className="field-label"
                ),
                min_consecutive_days_input,

                html.H3("Patient filters"),
                html.Div("ECOG", className="field-label"),
                ecog_multi_select,
                html.Div("Age", className="field-label"),
                age_slider,
                html.Div("Gender", className="field-label"),
                gender_multi_select,
                html.Div("Therapy", className="field-label"),
                therapy_multi_select,
                html.Div("Therapy Regimen", className="field-label"),
                therapy_regimen_multi_select,
                html.Div("Therapy Naive", className="field-label"),
                treatment_naive_multi_select,
                html.Div("Prior Treatment", className="field-label"),
                prior_treatment_multi_select,
                html.Div("Primary Tumor", className="field-label"),
                primary_tumor_multi_select,
                html.Div("Fitness: TUG", className="field-label"),
                tug_range_slider,
                tug_nan_checkbox,
                html.Div("Fitness: HGS", className="field-label"),
                hgs_range_slider,
                hgs_nan_checkbox,
            ]
        )

        return cls(
            body=body,
            out_min_hours_per_day_display=min_hours_per_day_display,
            in_min_days_input=min_days_input,
            in_min_consecutive_days_input=min_consecutive_days_input,
            in_ecog_multi_select=ecog_multi_select,
            in_min_hours_per_day_input=min_hours_per_day_input,
            in_age_slider=age_slider,
            in_gender_multi_select=gender_multi_select,
            in_therapy_multi_select=therapy_multi_select,
            in_therapy_regimen_multi_select=therapy_regimen_multi_select,
            in_treatment_naive_multi_select=treatment_naive_multi_select,
            in_prior_treatment_multi_select=prior_treatment_multi_select,
            in_primary_tumor_multi_select=primary_tumor_multi_select,
            in_tug_range_slider=tug_range_slider,
            in_tug_nan_checkbox=tug_nan_checkbox,
            in_hgs_range_slider=hgs_range_slider,
            in_hgs_nan_checkbox=hgs_nan_checkbox,
        )
