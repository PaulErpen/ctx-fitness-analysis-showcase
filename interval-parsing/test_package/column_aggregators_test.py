from typing import Any, List
import unittest
import pandas as pd
import datetime as dt
from ctxfitness.column_aggregators import COLUMN_AGGREGATOR_COMPUTED_DAILY_DURATION, COLUMN_AGGREGATOR_COMPUTED_FIRST_TIME_WORN_ON_DAY, COLUMN_AGGREGATOR_COMPUTED_LAST_TIME_WORN_ON_DAY, adjusted_mean_aggregator, aggregate_dailies_fractions, robust_unique_nan_aggregator, simple_appending_aggregator, simple_weighted_ordinal_aggregator, simple_unique_aggregator, fractured_sum_aggregator, stress_qualifier_aggregator
import numpy as np
from ctxfitness.preprocessing_pipeline import RAW_DATA_INTERVAL_END_TIME, RAW_DATA_INTERVAL_START_TIME, RAW_INTERVAL_DATA_COLUMN_FILTER, RAW_DAILIES_EXCEL_COLUMN_FILTER
from ctxfitness.time_utils import parse_datestr_interval_time
import ctxfitness.interval_parser as ip

SOME_NAN_PLACEHOLDER = "SOME_NAN_PLACEHOLDER"

class ColumnAggregatorsTest(unittest.TestCase):
    df_dailies: pd.DataFrame

    @classmethod
    def setUpClass(cls):
        cls.df_dailies = pd.read_excel(
            "../SampleData/Dailies 012.xlsx")[RAW_DAILIES_EXCEL_COLUMN_FILTER]
        
    def test_adjusted_mean_aggregator(self):
        adj_mean = adjusted_mean_aggregator(
            [dt.timedelta(1), dt.timedelta(1), dt.timedelta(2)],
            [dt.timedelta(1), dt.timedelta(1), dt.timedelta(2)],
            pd.Series([12, 12, 3])
        )
        self.assertEqual(adj_mean, 7.5)

    def test_simple_sum_aggregator(self):
        simple_sum = fractured_sum_aggregator(
            [dt.timedelta(1), dt.timedelta(1), dt.timedelta(1)],
            [dt.timedelta(1), dt.timedelta(1), dt.timedelta(1)],
            pd.Series([1, 2, 3])
        )
        self.assertEqual(simple_sum, 6)

    def test_simple_sum_aggregator_empty(self):
        simple_sum = fractured_sum_aggregator(
            [],
            [],
            pd.Series([])
        )
        self.assertEqual(simple_sum, 0)

    def test_simple_sum_aggregator_nan(self):
        simple_sum = fractured_sum_aggregator(
            [dt.timedelta(1)],
            [dt.timedelta(1)],
            pd.Series([np.NaN])
        )
        self.assertEqual(simple_sum, 0)
    
    def test_simple_sum_aggregator_diverging(self):
        simple_sum = fractured_sum_aggregator(
            [dt.timedelta(1), dt.timedelta(2), dt.timedelta(1)],
            [dt.timedelta(1), dt.timedelta(1), dt.timedelta(1)],
            pd.Series([1, 2, 3])
        )
        self.assertEqual(simple_sum, 5)

    def test_simple_unique_aggregator(self):
        first = simple_unique_aggregator(
            [],
            [],
            pd.Series(["one", "one", "one"])
        )
        self.assertEqual(first, "one")

    def test_simple_appending_aggregator(self):
        appended = simple_appending_aggregator(
            [],
            [],
            pd.Series(["hello", 123, "Yess"])
        )
        self.assertEqual(appended, "hello,123,Yess")

    def test_simple_appending_aggregator_empty(self):
        appended = simple_appending_aggregator(
            [],
            [],
            pd.Series([])
        )
        self.assertEqual(appended, "")
    
    def test_robust_unique_nan_aggregator_normal_case(self):
        value = robust_unique_nan_aggregator([], [], pd.Series([np.NAN, "Hello"]))
        self.assertEqual(value, "Hello")

    def test_robust_unique_nan_aggregator_all_same(self):
        value = robust_unique_nan_aggregator([], [], pd.Series(["Hello", "Hello"]))
        self.assertEqual(value, "Hello")
    
    def test_simple_weighted_ordinal_aggregator(self):
        agg_value = simple_weighted_ordinal_aggregator([dt.timedelta(9), dt.timedelta(20)],[dt.timedelta(9), dt.timedelta(20)], pd.Series(["ONE", "TWO"]))
        self.assertEqual(agg_value, "TWO")
    
    def test_simple_weighted_ordinal_aggregator_same(self):
        agg_value = simple_weighted_ordinal_aggregator([dt.timedelta(10), dt.timedelta(10)],[dt.timedelta(10), dt.timedelta(10)], pd.Series(["ONE", "TWO"]))
        self.assertEqual(agg_value, "ONE")
    
    def test_simple_weighted_ordinal_aggregator_all_nans(self):
        agg_value = simple_weighted_ordinal_aggregator([dt.timedelta(10), dt.timedelta(10)], [dt.timedelta(10), dt.timedelta(10)], pd.Series([np.NAN, np.NAN]))
        self.assertTrue(np.isnan(agg_value))
    
    def test_stress_qualifier_aggregator_not_in(self):
        agg_value = stress_qualifier_aggregator(
            [dt.timedelta(1), dt.timedelta(1), dt.timedelta(500)], 
            [dt.timedelta(1), dt.timedelta(1), dt.timedelta(500)], 
            pd.Series(["calm", "calm", "stressful"]))
        self.assertEqual(agg_value, "stressful")

    def test_stress_qualifier_aggregator_nan(self):
        agg_value = stress_qualifier_aggregator(
            [dt.timedelta(1), dt.timedelta(1), dt.timedelta(0), dt.timedelta(500)], 
            [dt.timedelta(1), dt.timedelta(1), dt.timedelta(0), dt.timedelta(500)], 
            pd.Series(["calm", "calm", np.NAN, "stressful"]))
        self.assertEqual(agg_value, "stressful")

    def test_stress_qualifier_aggregator_all_nan(self):
        agg_value = stress_qualifier_aggregator([dt.timedelta(0)],[dt.timedelta(0)], pd.Series([np.NAN]))
        self.assertTrue(np.isnan(agg_value))
    
    def test_stress_qualifier_aggregator_single_0(self):
        agg_value = stress_qualifier_aggregator([dt.timedelta(0)],[dt.timedelta(0)], pd.Series(["stressful"]))
        self.assertTrue(np.isnan(agg_value))
    
    def test_aggregate_dailies_fractions(self):
        some_interval: ip.Interval = ip.Interval(
            parse_datestr_interval_time(
                self.df_dailies.iloc[0][RAW_DATA_INTERVAL_START_TIME]),
            parse_datestr_interval_time(
                self.df_dailies.iloc[0][RAW_DATA_INTERVAL_END_TIME]),
            self.df_dailies.iloc[0]
        )
        some_other_interval: ip.Interval = ip.Interval(
            parse_datestr_interval_time(
                self.df_dailies.iloc[1][RAW_DATA_INTERVAL_START_TIME]),
            parse_datestr_interval_time(
                self.df_dailies.iloc[1][RAW_DATA_INTERVAL_END_TIME]),
            self.df_dailies.iloc[1]
        )
        interval_fractions: List[ip.IntervalFraction] = [
            ip.IntervalFraction(
                some_interval,
                dt.timedelta(1)
            ),
            ip.IntervalFraction(
                some_other_interval,
                dt.timedelta(1)
            )
        ]
        aggregated_series: "pd.Series[Any]" = aggregate_dailies_fractions(
            interval_fractions)

        new_index = RAW_INTERVAL_DATA_COLUMN_FILTER.copy()
        new_index.extend([COLUMN_AGGREGATOR_COMPUTED_DAILY_DURATION,
                          COLUMN_AGGREGATOR_COMPUTED_FIRST_TIME_WORN_ON_DAY,
                          COLUMN_AGGREGATOR_COMPUTED_LAST_TIME_WORN_ON_DAY])

        self.assertTrue(
            aggregated_series.fillna(SOME_NAN_PLACEHOLDER).equals(
                pd.Series([
                    "60ec609ef300c47533539a86",
                    12,
                    "Pilot Study Physical Fitness in Cancer Patients",
                    86400 * 2,
                    "4998,4999",
                    SOME_NAN_PLACEHOLDER,
                    723 + 653,
                    450.2099999999999 + 406.6500000000001,
                    0,
                    0,
                    0,
                    SOME_NAN_PLACEHOLDER,
                    0,
                    SOME_NAN_PLACEHOLDER,
                    SOME_NAN_PLACEHOLDER,
                    SOME_NAN_PLACEHOLDER,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    SOME_NAN_PLACEHOLDER,
                    86400 * 2,
                    dt.datetime(2021, 8, 1),
                    dt.datetime(2021, 8, 3)
                ], index=new_index)
            )
        )