from typing import Any, Callable, Dict, List, Union
import datetime as dt
import pandas as pd
import numpy as np
import ctxfitness.interval_parser as ip

def fractured_sum_aggregator(totals: List[dt.timedelta], daily_fractions: List[dt.timedelta], series: "pd.Series[Any]"):
    tups: list[tuple[dt.timedelta, dt.timedelta]] = list(zip(totals, daily_fractions))
    fracs: list[float] = [fraction.total_seconds() / total.total_seconds() for total, fraction in tups]
    weighted_series_values = []
    series = series.fillna(0)
    for i in range(len(series)):
        weighted_series_values.append(fracs[i] * series.iloc[i])
    return sum(weighted_series_values)


def simple_unique_aggregator(totals: List[dt.timedelta], daily_fractions: List[dt.timedelta], series: "pd.Series[Any]"):
    unique = series.unique()
    if len(unique) != 1:
        raise Exception(
            f"The series with the unique values '{unique}' has more than one value! This is unexpected so it leads to an exception!")
    return series.iloc[0]


def adjusted_mean_aggregator(totals: List[dt.timedelta], daily_fractions: List[dt.timedelta], series: "pd.Series[Any]"):
    total = sum(map(lambda td: td.total_seconds(), daily_fractions))
    adj_mean = 0
    for i in range(len(daily_fractions)):
        adj_mean = adj_mean + \
            daily_fractions[i].total_seconds() / total * series.iloc[i]
    return adj_mean


def simple_appending_aggregator(totals: List[dt.timedelta], daily_fractions: List[dt.timedelta], series: "pd.Series[Any]") -> str:
    ret = ""
    for i in range(len(series)):
        if i > 0:
            ret = ret + ","
        ret = ret + str(series.iloc[i])
    return ret


def robust_unique_nan_aggregator(totals: List[dt.timedelta], daily_fractions: List[dt.timedelta], series: "pd.Series[Any]") -> Union[Any, float]:
    unique = series.unique()
    if len(unique) == 1:
        return series.iloc[0]
    n_not_na = series.notna().sum()
    if n_not_na > 1:
        raise Exception(
            f"There is more than one non-nan value present in the series '{series}'. This is unexpected so it leads to an exception!")
    if n_not_na == 0:
        return np.nan
    for value in series.dropna().values:
        return value
    return np.nan


stress_dict = {
    "calm": 1,
    "balanced": 2,
    "stressful": 3,
    "very_stressful": 4
}
inverted_stress_dict = {v: k for k, v in stress_dict.items()}


def stress_qualifier_aggregator(totals: List[dt.timedelta], daily_fractions: List[dt.timedelta], series: "pd.Series[Any]") -> Union[str, float]:
    if series.notna().sum() < 1:
        return np.nan
    if set(set(series.notna().unique())).difference(stress_dict.keys()) == set():
        raise Exception(
            f"The set of stress levels {series} contains an unrecognized stress level! This should not be!")
    nan_helper_df = pd.DataFrame.from_dict({
        "delta_s": pd.Series(daily_fractions).apply(lambda delta: delta.total_seconds()),
        "parsed_stress_levels": series.apply(lambda x: stress_dict[x] if x is not np.nan else np.nan)
    }).dropna()
    if nan_helper_df.delta_s.sum() == 0:
        return np.nan
    weighted_average = np.average(
        nan_helper_df.parsed_stress_levels, weights=nan_helper_df.delta_s)
    return inverted_stress_dict[round(weighted_average)]


def simple_weighted_ordinal_aggregator(totals: List[dt.timedelta], daily_fractions: List[dt.timedelta], series: "pd.Series[Any]"):
    nan_helper_df = (pd.DataFrame
                     .from_dict({
                         "delta_s": pd.Series(daily_fractions).apply(lambda delta: delta.total_seconds()),
                         "nominal_values": series
                     })
                     .dropna()
                     .groupby("nominal_values")
                     .sum())
    if nan_helper_df.empty:
        return np.nan
    return nan_helper_df.idxmax().iloc[0]

column_aggregation_strategy: Dict[str, Callable[[List[dt.timedelta], List[dt.timedelta], "pd.Series[Any]"], Any]] = {
    "User Id": simple_unique_aggregator,
    "User Last Name": simple_unique_aggregator,
    "Group Names": simple_unique_aggregator,
    "Duration (s)": fractured_sum_aggregator,
    "Summary Id": simple_appending_aggregator,
    "Activity Type": simple_weighted_ordinal_aggregator,
    "Steps": fractured_sum_aggregator,
    "Distance  (m)": fractured_sum_aggregator,
    "Moderate Intensity Duration (s)": fractured_sum_aggregator,
    "Vigorous Intensity Duration (s)": fractured_sum_aggregator,
    "Floors Climbed": fractured_sum_aggregator,
    "Heart Rate (min bpm)": lambda totals, daily_fractions, series: series.min(),
    "Heart Rate (avg bpm)": adjusted_mean_aggregator,
    "Heart Rate (max bpm)": lambda totals,daily_fractions, series: series.max(),
    "Stress Level (avg)": adjusted_mean_aggregator,
    "Stress Level (max)": lambda totals, daily_fractions, series: series.max(),
    "Stress Duration (s)": fractured_sum_aggregator,
    "Rest Stress Duration (s)": fractured_sum_aggregator,
    "Activity Stress Duration (s)": fractured_sum_aggregator,
    "Low Stress Duration (s)": fractured_sum_aggregator,
    "Medium Stress Duration (s)": fractured_sum_aggregator,
    "High Stress Duration (s)": fractured_sum_aggregator,
    "Stress Qualifier": stress_qualifier_aggregator
}

COLUMN_AGGREGATOR_COMPUTED_DAILY_DURATION = "actual_daily_duration_s"
COLUMN_AGGREGATOR_COMPUTED_FIRST_TIME_WORN_ON_DAY = "first_time_worn_on_day"
COLUMN_AGGREGATOR_COMPUTED_LAST_TIME_WORN_ON_DAY = "last_time_worn_on_day"

def aggregate_dailies_fractions(intervals: List[ip.IntervalFraction]) -> "pd.Series[Any]":
    df: pd.DataFrame = pd.concat(
        list(map(lambda i: i.interval.data, intervals)),
        axis=1
    ).transpose()
    aggregated_series: List[Any] = []
    index_of_aggs: List[str] = []
    for colname in df.columns:
        if colname in column_aggregation_strategy:
            agg_value = column_aggregation_strategy[colname](
                list(map(lambda ivf: ivf.interval.end - ivf.interval.start, intervals)),
                list(map(lambda ivf: ivf.fraction, intervals)), 
                df[colname])
            aggregated_series.append(agg_value)
            index_of_aggs.append(colname)

    # Extra metadata
    index_of_aggs.append(COLUMN_AGGREGATOR_COMPUTED_DAILY_DURATION)
    aggregated_series.append(
        sum(map(lambda ivf: ivf.fraction.total_seconds(), intervals))
    )

    index_of_aggs.append(COLUMN_AGGREGATOR_COMPUTED_FIRST_TIME_WORN_ON_DAY)
    aggregated_series.append(
        min(map(lambda ivf: ivf.interval.start, intervals))
    )

    index_of_aggs.append(COLUMN_AGGREGATOR_COMPUTED_LAST_TIME_WORN_ON_DAY)
    aggregated_series.append(
        max(map(lambda ivf: ivf.interval.end, intervals))
    )

    return pd.Series(aggregated_series, index=index_of_aggs)