import enum
from typing import Callable, Dict
import pandas as pd
from dash import dcc


class InitialSelection(enum.Enum):
    ALL = "ALL",
    NONE = "NONE"

class LabelingStrategy(enum.Enum):
    AS_STRING = "AS_STRING",
    AS_INTEGER = "AS_INTEGER"


labeling_strategies: Dict[LabelingStrategy, Callable] = {
    LabelingStrategy.AS_STRING: lambda value: str(value) if not pd.isna(value) else "Unknown",
    LabelingStrategy.AS_INTEGER: lambda value: str(int(
        value)) if not pd.isna(value) else "Unknown"
}


def get_series_dropdown(id: str, series: pd.Series, initial_selection: InitialSelection, labeling_strategy: LabelingStrategy, multi: bool) -> dcc.Dropdown:
    options = [
        {
            "label": labeling_strategies[labeling_strategy](unique_value),
            "value": str(unique_value)
        } for unique_value in series.unique()
    ]
    options = sorted(options, key=lambda option: option["label"])
    selected = []
    if initial_selection == InitialSelection.ALL and multi:
        selected = [opt["value"] for opt in options]
    
    return dcc.Dropdown(
        id=id,
        options=options,
        value=selected,
        multi=multi
    )