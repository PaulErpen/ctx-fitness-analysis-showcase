from dataclasses import dataclass
from math import log
from typing import List, Tuple
import plotly.graph_objects as go
from ctxfitness.ctx_patient_dailies_handler import CTxPatientDailiesHandler, CTxPatientDay
import numpy as np


@dataclass
class PreparedHeatmap:
    durations_matrix: np.ndarray
    hover_matrix: np.ndarray
    y_ticks: List[str]
    x_ticks: List[str]

    @staticmethod
    def prepare_hover_matrix(array_shape: Tuple[int, int]) -> np.ndarray:
        hover_matrix = np.empty(array_shape, dtype="object")
        for i in range(0, array_shape[0]):
            for j in range(0, array_shape[1]):
                hover_matrix[i,
                             j] = f"Day {j+1}: {CTxPatientDay.ZERO_TIME_STRING}"
        return hover_matrix

    @classmethod
    def prepare_heatmap(cls, patient_entries: List[CTxPatientDailiesHandler], accept_day_hours: int) -> "PreparedHeatmap":
        max_number_durations = max(
            [len(pat_entry.get_durations()) for pat_entry in patient_entries])
        array_shape = (len(patient_entries), max_number_durations)
        durations_matrix = np.zeros(array_shape)
        hover_matrix = cls.prepare_hover_matrix(array_shape)
        for i, pat_entry in enumerate(patient_entries):
            for j, duration in enumerate(pat_entry.get_durations()):
                durations_matrix[i, j] = min(
                    duration / (accept_day_hours * 3600), 1)
                hover_matrix[i,
                             j] = f"Day {j+1}: {CTxPatientDay.format_daily_seconds(duration)}"
        return cls(
            durations_matrix=durations_matrix,
            hover_matrix=hover_matrix,
            x_ticks=[f"Day {i + 1}" for i in range(0, max_number_durations)],
            y_ticks=[f" {pat.patient_id} -" for pat in patient_entries]
        )


def render_times_heatmap(patient_entries: List[CTxPatientDailiesHandler], accept_day_hours: int) -> go.Figure:
    prepared_heatmap = PreparedHeatmap.prepare_heatmap(
        patient_entries, accept_day_hours)
    fig = go.Figure(
        data=go.Heatmap(
            z=prepared_heatmap.durations_matrix,
            coloraxis=None,
            hovertemplate="%{customdata}<extra></extra>",
            customdata=prepared_heatmap.hover_matrix,
            zmax=1,
            zmin=0,
            colorscale=[(0, "#000000"), (0.9999, "#e3fc03"),
                        (1, "#5afc03")]
        )
    )
    selected_x_ticks = [i for i in range(0, len(
        prepared_heatmap.x_ticks), 1 + int(log(len(prepared_heatmap.x_ticks), 2)))]
    fig.update_layout(dict(
        xaxis={
            'showgrid': False,
            'zeroline': False,
            'visible': False,
            'fixedrange': True
        },
        yaxis=dict(
            tickmode='array',
            tickvals=[i for i in range(len(prepared_heatmap.y_ticks))],
            ticktext=prepared_heatmap.y_ticks
        ),
        margin={
            "l": 0,
            "r": 0,
            "b": 10,
            "t": 10
        }
    ))
    fig.update_coloraxes(showscale=False)
    fig.update_traces(showscale=False)
    return fig


@dataclass
class PreparedAcceptanceHeatMap:
    acceptance_values: np.ndarray
    hover_matrix: np.ndarray
    text_matrix: np.ndarray

    @classmethod
    def create_prepared_acceptance_matrix(cls, patient_entries: List[CTxPatientDailiesHandler]) -> "PreparedAcceptanceHeatMap":
        return cls(
            np.array([1 if p.accepted else 0 for p in patient_entries])
            .reshape((len(patient_entries), 1)),
            np.array(
                [f"{p.patient_id} accepted" if p.accepted else f"{p.patient_id} not accepted" for p in patient_entries])
            .reshape((len(patient_entries), 1)),
            np.array(
                [f"✓" if p.accepted else f"x" for p in patient_entries])
            .reshape((len(patient_entries), 1))
        )


def render_acceptance_heatmap(patient_entries: List[CTxPatientDailiesHandler]) -> go.Figure:
    prepared_heatmap = PreparedAcceptanceHeatMap.create_prepared_acceptance_matrix(
        patient_entries)
    fig = go.Figure(
        data=go.Heatmap(
            z=prepared_heatmap.acceptance_values,
            text=prepared_heatmap.text_matrix,
            texttemplate="%{text}",
            textfont={"size": 10},
            coloraxis=None,
            zmax=1,
            zmin=0,
            colorscale=[(0, "#fc0303"), (1, "#5afc03")],
            hoverinfo="skip"
        )
    )
    fig.update_layout(dict(
        xaxis={
            'showgrid': False,
            'zeroline': False,
            'visible': False,
            'fixedrange': True
        },
        yaxis={
            'showgrid': False,
            'zeroline': False,
            'visible': False,
            'fixedrange': True
        },
        margin={
            "l": 0,
            "r": 0,
            "b": 10,
            "t": 10
        }
    ))
    fig.update_coloraxes(showscale=False)
    fig.update_traces(showscale=False)
    return fig
