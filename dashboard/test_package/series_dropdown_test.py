import unittest
import pandas as pd
from ctxdashboard.components.series_dropdown import get_series_dropdown, InitialSelection, LabelingStrategy
import numpy as np


class SeriesDropdownTest(unittest.TestCase):
    def test_given_integer_and_multi_all_selected(self):
        drop_down = get_series_dropdown(
            id="someid",
            series=pd.Series([1.0, 123.21312]),
            initial_selection=InitialSelection.ALL,
            labeling_strategy=LabelingStrategy.AS_INTEGER,
            multi=True,
        )

        self.assertSequenceEqual(
            drop_down.options,  # type: ignore
            [
                {"label": "1", "value": "1.0"},
                {"label": "123", "value": "123.21312"},
            ]
        )
        self.assertSequenceEqual(
            drop_down.value,  # type: ignore
            [
                "1.0",
                "123.21312",
            ]
        )

    def test_given_integer_and_no_multi_all_selected(self):
        drop_down = get_series_dropdown(
            id="someid",
            series=pd.Series([1.0, 123.21312]),
            initial_selection=InitialSelection.ALL,
            labeling_strategy=LabelingStrategy.AS_INTEGER,
            multi=False,
        )

        self.assertSequenceEqual(
            drop_down.options,  # type: ignore
            [
                {"label": "1", "value": "1.0"},
                {"label": "123", "value": "123.21312"},
            ]
        )
        self.assertSequenceEqual(
            drop_down.value,  # type: ignore
            []
        )

    def test_given_integer_and_no_multi_none_selected(self):
        drop_down = get_series_dropdown(
            id="someid",
            series=pd.Series([1.0, 123.21312]),
            initial_selection=InitialSelection.NONE,
            labeling_strategy=LabelingStrategy.AS_INTEGER,
            multi=True,
        )

        self.assertSequenceEqual(
            drop_down.options,  # type: ignore
            [
                {"label": "1", "value": "1.0"},
                {"label": "123", "value": "123.21312"},
            ]
        )
        self.assertSequenceEqual(
            drop_down.value,  # type: ignore
            []
        )

    def test_given_string_and_no_multi_none_selected(self):
        drop_down = get_series_dropdown(
            id="someid",
            series=pd.Series([1.0, 123.21312]),
            initial_selection=InitialSelection.NONE,
            labeling_strategy=LabelingStrategy.AS_STRING,
            multi=True,
        )

        self.assertSequenceEqual(
            drop_down.options,  # type: ignore
            [
                {"label": "1.0", "value": "1.0"},
                {"label": "123.21312", "value": "123.21312"},
            ]
        )
        self.assertSequenceEqual(
            drop_down.value,  # type: ignore
            []
        )

    def test_given_string_and_multi_and_all_and_nans(self):
        drop_down = get_series_dropdown(
            id="someid",
            series=pd.Series([1.0, 123.21312, np.nan]),
            initial_selection=InitialSelection.ALL,
            labeling_strategy=LabelingStrategy.AS_STRING,
            multi=True,
        )

        self.assertSequenceEqual(
            drop_down.options,  # type: ignore
            [
                {"label": "1.0", "value": "1.0"},
                {"label": "123.21312", "value": "123.21312"},
                {"label": "Unknown", "value": "nan"}
            ]
        )
        self.assertSequenceEqual(
            drop_down.value,  # type: ignore
            ["1.0", "123.21312", "nan"]
        )

    def test_sorting_string(self):
        drop_down = get_series_dropdown(
            id="someid",
            series=pd.Series(["XYZ", "ABC"]),
            initial_selection=InitialSelection.NONE,
            labeling_strategy=LabelingStrategy.AS_STRING,
            multi=True,
        )

        self.assertSequenceEqual(
            drop_down.options,  # type: ignore
            [
                {"label": "ABC", "value": "ABC"},
                {"label": "XYZ", "value": "XYZ"},
            ]
        )
        self.assertSequenceEqual(
            drop_down.value,  # type: ignore
            []
        )
