from enum import Enum
from typing import Any, Dict, List, Union
from ctxfitness.time_utils import parse_datestr_interval_time
import pandas as pd
import ctxfitness.interval_parser as ip
from ctxfitness.column_aggregators import COLUMN_AGGREGATOR_COMPUTED_DAILY_DURATION, COLUMN_AGGREGATOR_COMPUTED_FIRST_TIME_WORN_ON_DAY, COLUMN_AGGREGATOR_COMPUTED_LAST_TIME_WORN_ON_DAY, aggregate_dailies_fractions

# filter to apply to data passed to dailies intervals
RAW_INTERVAL_DATA_COLUMN_FILTER: List[str] = [
    "User Id",
    "User Last Name",
    "Group Names",
    "Duration (s)",
    "Summary Id",
    "Activity Type",
    "Steps",
    "Distance  (m)",
    "Moderate Intensity Duration (s)",
    "Vigorous Intensity Duration (s)",
    "Floors Climbed",
    "Heart Rate (min bpm)",
    "Heart Rate (avg bpm)",
    "Heart Rate (max bpm)",
    "Stress Level (avg)",
    "Stress Level (max)",
    "Stress Duration (s)",
    "Rest Stress Duration (s)",
    "Activity Stress Duration (s)",
    "Low Stress Duration (s)",
    "Medium Stress Duration (s)",
    "High Stress Duration (s)",
    "Stress Qualifier"
]

RAW_DATA_INTERVAL_START_TIME = "Start Time (Local)"
RAW_DATA_INTERVAL_END_TIME = "End Time (Local)"

# Filter to apply to raw dailies excel
RAW_DAILIES_EXCEL_COLUMN_FILTER: List[str] = [
    RAW_DATA_INTERVAL_START_TIME, RAW_DATA_INTERVAL_END_TIME] + RAW_INTERVAL_DATA_COLUMN_FILTER

class ParsedDailiesColumns(str, Enum):
    # Convenience column enum for better IntelliSense
    USER_LAST_NAME = "Tracker ID",
    GROUP_NAMES = "Group Names",
    _FLAWED_DURATION_S = "BROKEN Duration (s)",
    SUMMARY_ID = "Summary Id",
    ACTIVITY_TYPE = "Activity Type",
    STEPS = "Steps",
    DISTANCE_M = "Distance  (m)",
    MODERATE_INTENSITY_DURATION_S = "Moderate Intensity Duration (s)",
    VIGOROUS_INTENSITY_DURATION_S = "Vigorous Intensity Duration (s)",
    FLOORS_CLIMBED = "Floors Climbed",
    HEART_RATE_MIN_BPM = "Heart Rate (min bpm)",
    HEART_RATE_AVG_BPM = "Heart Rate (avg bpm)",
    HEART_RATE_MAX_BPM = "Heart Rate (max bpm)",
    STRESS_LEVEL_AVG = "Stress Level (avg)",
    STRESS_LEVEL_MAX = "Stress Level (max)",
    STRESS_DURATION_S = "Stress Duration (s)",
    REST_STRESS_DURATION_S = "Rest Stress Duration (s)",
    ACTIVITY_STRESS_DURATION_S = "Activity Stress Duration (s)",
    LOW_STRESS_DURATION_S = "Low Stress Duration (s)",
    MEDIUM_STRESS_DURATION_S = "Medium Stress Duration (s)",
    HIGH_STRESS_DURATION_S = "High Stress Duration (s)",
    STRESS_QUALIFIER = "Stress Qualifier",
    START_DT = "Day start (timestamp)",
    END_DT = "Day end (timestamp)",
    DAILY_DURATION_S = "Computed daily duration (s)",
    FIRST_TIME_WORN_ON_DAY = "Computed first time worn on day",
    LAST_TIME_WORN_ON_DAY = "Computed last time worn on day"

PARSED_DAILIES_COLUMN_RENAMER: Dict[str, str] = {
    "User Last Name": f"{ParsedDailiesColumns.USER_LAST_NAME.value}",
    "Group Names": f"{ParsedDailiesColumns.GROUP_NAMES.value}",
    "Duration (s)": f"{ParsedDailiesColumns._FLAWED_DURATION_S.value}",
    "Summary Id": f"{ParsedDailiesColumns.SUMMARY_ID.value}",
    "Activity Type": f"{ParsedDailiesColumns.ACTIVITY_TYPE.value}",
    "Steps": f"{ParsedDailiesColumns.STEPS.value}",
    "Distance  (m)": f"{ParsedDailiesColumns.DISTANCE_M.value}",
    "Moderate Intensity Duration (s)": f"{ParsedDailiesColumns.MODERATE_INTENSITY_DURATION_S.value}",
    "Vigorous Intensity Duration (s)": f"{ParsedDailiesColumns.VIGOROUS_INTENSITY_DURATION_S.value}",
    "Floors Climbed": f"{ParsedDailiesColumns.FLOORS_CLIMBED.value}",
    "Heart Rate (min bpm)": f"{ParsedDailiesColumns.HEART_RATE_MIN_BPM.value}",
    "Heart Rate (avg bpm)": f"{ParsedDailiesColumns.HEART_RATE_AVG_BPM.value}",
    "Heart Rate (max bpm)": f"{ParsedDailiesColumns.HEART_RATE_MAX_BPM.value}",
    "Stress Level (avg)": f"{ParsedDailiesColumns.STRESS_LEVEL_AVG.value}",
    "Stress Level (max)": f"{ParsedDailiesColumns.STRESS_LEVEL_MAX.value}",
    "Stress Duration (s)": f"{ParsedDailiesColumns.STRESS_DURATION_S.value}",
    "Rest Stress Duration (s)": f"{ParsedDailiesColumns.REST_STRESS_DURATION_S.value}",
    "Activity Stress Duration (s)": f"{ParsedDailiesColumns.ACTIVITY_STRESS_DURATION_S.value}",
    "Low Stress Duration (s)": f"{ParsedDailiesColumns.LOW_STRESS_DURATION_S.value}",
    "Medium Stress Duration (s)": f"{ParsedDailiesColumns.MEDIUM_STRESS_DURATION_S.value}",
    "High Stress Duration (s)": f"{ParsedDailiesColumns.HIGH_STRESS_DURATION_S.value}",
    "Stress Qualifier": f"{ParsedDailiesColumns.STRESS_QUALIFIER.value}",
    "start_dt": f"{ParsedDailiesColumns.START_DT.value}",
    "end_dt": f"{ParsedDailiesColumns.END_DT.value}",
    COLUMN_AGGREGATOR_COMPUTED_DAILY_DURATION:  f"{ParsedDailiesColumns.DAILY_DURATION_S.value}",
    COLUMN_AGGREGATOR_COMPUTED_FIRST_TIME_WORN_ON_DAY:  f"{ParsedDailiesColumns.FIRST_TIME_WORN_ON_DAY.value}",
    COLUMN_AGGREGATOR_COMPUTED_LAST_TIME_WORN_ON_DAY:  f"{ParsedDailiesColumns.LAST_TIME_WORN_ON_DAY.value}"
}

class PreprocessingPipeline:
    @staticmethod
    def run_pipeline(path: str):
        df_raw = PreprocessingPipeline.parse_and_load_multiple_patients_df(path)
        return PreprocessingPipeline.rename_and_restrict_columns(df_raw)

    @staticmethod 
    def rename_and_restrict_columns(df: pd.DataFrame) -> pd.DataFrame:
        df_processed = df.copy()
        df_processed = df_processed.rename(PARSED_DAILIES_COLUMN_RENAMER, axis=1)
        columns = [e.value for e in ParsedDailiesColumns]
        df_processed = df_processed[columns]
        return df_processed

    @staticmethod
    def interval_from_dailies_row(row: "pd.Series[Any]") -> ip.Interval:
        return ip.Interval(
            parse_datestr_interval_time(row[RAW_DATA_INTERVAL_START_TIME]),
            parse_datestr_interval_time(row[RAW_DATA_INTERVAL_END_TIME]),
            row[RAW_INTERVAL_DATA_COLUMN_FILTER]
        )

    @staticmethod
    def interval_parse_dailies(df: pd.DataFrame) -> pd.DataFrame:
        df_lean = df[RAW_DAILIES_EXCEL_COLUMN_FILTER]
        intervals: List[ip.Interval] = []
        for _label, row in df_lean.iterrows():
            intervals.append(PreprocessingPipeline.interval_from_dailies_row(row))
        interval_normalizer: ip.IntervalNormalizer = ip.IntervalNormalizer(
            intervals)
        normalized_intervals: List[ip.Interval] = interval_normalizer.normalize_by_day(
            aggregate_dailies_fractions)
        df_ret: pd.DataFrame = pd.concat(
            [i.data for i in normalized_intervals],
            axis=1
        ).transpose()
        df_ret["start_dt"] = pd.Series([i.start for i in normalized_intervals])
        df_ret["end_dt"] = pd.Series([i.end for i in normalized_intervals])
        return df_ret

    @staticmethod
    def parse_and_load_multiple_patients_df(path: str) -> pd.DataFrame:
        multi_dailies_df = pd.read_excel(path)
        user_ids = multi_dailies_df["User Id"].unique()
        parsed_dailies_list: List[pd.DataFrame] = []
        for user_id in user_ids:
            try:
                parsed_dailies_list.append(PreprocessingPipeline.interval_parse_dailies(
                    multi_dailies_df[multi_dailies_df["User Id"] == user_id]))
            except Exception as excpetion:
                raise Exception(
                    f"The following error occurred for the patient with the id '{user_id}': {excpetion}")
        return pd.concat(parsed_dailies_list)