import os
from typing import Any
import unittest
import ctxfitness.interval_parser as ip
import pandas as pd
import datetime as dt

from ctxfitness.preprocessing_pipeline import RAW_INTERVAL_DATA_COLUMN_FILTER, RAW_DAILIES_EXCEL_COLUMN_FILTER, PreprocessingPipeline, ParsedDailiesColumns as pdc

SOME_NAN_PLACEHOLDER = "SOME_NAN_PLACEHOLDER"


class PreprocessingPipelineTest(unittest.TestCase):
    df_dailies: pd.DataFrame
    some_datetime: dt.datetime
    first_row_series: "pd.Series[Any]"

    @classmethod
    def setUpClass(cls):
        cls.some_datetime = dt.datetime(2022, 12, 12)
        cls.df_dailies = pd.read_excel(
            "../SampleData/Dailies 012.xlsx")[RAW_DAILIES_EXCEL_COLUMN_FILTER]
        cls.first_row_series = pd.Series([
            "60ec609ef300c47533539a86",
            12,
            "Pilot Study Physical Fitness in Cancer Patients",
            86400,
            "4998",
            SOME_NAN_PLACEHOLDER,
            723,
            450.2099999999999,
            0,
            0,
            0,
            SOME_NAN_PLACEHOLDER,
            0,
            SOME_NAN_PLACEHOLDER,
            SOME_NAN_PLACEHOLDER,
            SOME_NAN_PLACEHOLDER,
            SOME_NAN_PLACEHOLDER,
            SOME_NAN_PLACEHOLDER,
            SOME_NAN_PLACEHOLDER,
            SOME_NAN_PLACEHOLDER,
            SOME_NAN_PLACEHOLDER,
            SOME_NAN_PLACEHOLDER,
            SOME_NAN_PLACEHOLDER
        ], index=RAW_INTERVAL_DATA_COLUMN_FILTER)

    def test_interval_from_dailies_row(self):
        first_processed_row: ip.Interval = PreprocessingPipeline.interval_from_dailies_row(
            self.df_dailies.iloc[0])
        self.assertEqual(first_processed_row.start, dt.datetime(2021, 8, 1))
        self.assertEqual(first_processed_row.end, dt.datetime(2021, 8, 2))
        self.assertTrue(
            first_processed_row.data.fillna(
                SOME_NAN_PLACEHOLDER).equals(self.first_row_series)
        )

    def test_parsed_dailies_columns(self):
        df = PreprocessingPipeline.interval_parse_dailies(self.df_dailies.iloc[[0, 1]])
        df = PreprocessingPipeline.rename_and_restrict_columns(df)
        self.assertCountEqual(
            df[pdc.DISTANCE_M],
            pd.Series([
                450.2099999999999,
                406.6500000000001
            ])
        )
        self.assertCountEqual(
            df[pdc.STEPS],
            pd.Series([
                723,
                653
            ])
        )
        self.assertCountEqual(
            df[pdc.ACTIVITY_TYPE].notna(),
            pd.Series([
                False,
                False
            ])
        )
        self.assertCountEqual(
            df[pdc.HEART_RATE_AVG_BPM],
            pd.Series([
                0,
                0
            ])
        )
        self.assertCountEqual(
            df[pdc.SUMMARY_ID],
            pd.Series([
                "4998",
                "4999"
            ])
        )
        self.assertCountEqual(
            df[pdc.DAILY_DURATION_S],
            pd.Series([
                60 * 60 * 24,
                60 * 60 * 24
            ])
        )
        self.assertCountEqual(
            df[pdc.GROUP_NAMES],
            pd.Series([
                "Pilot Study Physical Fitness in Cancer Patients",
                "Pilot Study Physical Fitness in Cancer Patients"
            ])
        )

    def test_parse_and_load_multiple_patients_df_shape(self):
        # given subset of all data
        tmp_data_set_path: str = "./tmp_test_parsing.xlsx"
        subset_df = pd.read_excel("../SampleData/Dailies_ALL.xlsx").sample(n=1000, random_state=892173)
        subset_df.to_excel(tmp_data_set_path)

        df = PreprocessingPipeline.parse_and_load_multiple_patients_df(tmp_data_set_path)
        self.assertEqual(
            df.shape[1],
            len(pdc)
        )

        # cleanup
        os.remove(tmp_data_set_path)


if __name__ == '__main__':
    unittest.main()
