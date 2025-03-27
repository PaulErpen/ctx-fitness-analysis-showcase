from typing import List, Tuple, Union
import pandas as pd
from ctxdashboard.domain.patientmeta import PatientMetaColumn as pmc


def filter_patients(patient_cofactors: pd.DataFrame,
                    ecog_values: Union[List[str], None],
                    age_interval: Union[Tuple[str, str], None],
                    gender_multi_select_values: Union[List[str], None],
                    therapy_multi_select: Union[List[str], None],
                    therapy_regimen_multi_select: Union[List[str], None],
                    treatment_naive_multi_select: Union[List[str], None],
                    prior_treatment_multi_select: Union[List[str], None],
                    primary_tumor_multi_select: Union[List[str], None],
                    tug_range_slider: Union[Tuple[str, str], None],
                    tug_include_nans: Union[List[str], None],
                    hgs_range_slider: Union[Tuple[str, str], None],
                    hgs_include_nans: Union[List[str], None],) -> List[str]:

    df_filtered = patient_cofactors.copy()

    df_filtered = filter_by_float_interval(df_filtered, pmc.AGE, age_interval)

    df_filtered = filter_by_string_list(
        df_filtered, pmc.GENDER, gender_multi_select_values)

    df_filtered = filter_by_float_list(df_filtered, pmc.ECOG, ecog_values)

    df_filtered = filter_by_string_list(
        df_filtered, pmc.THERAPY, therapy_multi_select)

    df_filtered = filter_by_string_list(
        df_filtered, pmc.THERAPY_REGIMEN, therapy_regimen_multi_select)

    df_filtered = filter_by_string_list(
        df_filtered, pmc.TREATMENT_NAIVE, treatment_naive_multi_select)

    df_filtered = filter_by_string_list(
        df_filtered, pmc.PRIOR_TREATMENT, prior_treatment_multi_select)

    df_filtered = filter_by_string_list(
        df_filtered, pmc.PRIMARY_TUMOR, primary_tumor_multi_select)

    include_tug_nans = (tug_include_nans is not None and len(tug_include_nans) > 0)
    df_filtered = filter_by_float_interval(
        df_filtered, pmc.TUG, tug_range_slider, include_nans=include_tug_nans)

    include_hgs_nans = (hgs_include_nans is not None and len(hgs_include_nans) > 0)
    df_filtered = filter_by_float_interval(
        df_filtered, pmc.HGS, hgs_range_slider, include_nans=include_hgs_nans)

    return [str(i) for i in df_filtered[pmc.TRACKER_ID].unique()]


def filter_by_float_list(df_filtered: pd.DataFrame, column_name: str, values: Union[List[str], None]) -> pd.DataFrame:
    if values is None or len(values) == 0:
        return df_filtered
    parsed_values = [float(val) for val in values]
    return df_filtered[df_filtered[column_name].isin(parsed_values)]


def filter_by_float_interval(
        df_filtered: pd.DataFrame,
        column_name: str,
        interval: Union[Tuple[str, str], None],
        include_nans: bool = False
) -> pd.DataFrame:
    if interval is None:
        return df_filtered
    parsed_interval: Tuple[float, float] = (
        float(interval[0]), float(interval[1]))
    is_in_interval = ((df_filtered[column_name] >= parsed_interval[0])
                      & (df_filtered[column_name] <= parsed_interval[1]))
    is_included_as_nan = (include_nans & pd.isna(df_filtered[column_name]))
    return df_filtered[is_in_interval | is_included_as_nan]


def filter_by_string_list(df_filtered: pd.DataFrame, column_name: str, strings: Union[List[str], None]) -> pd.DataFrame:
    if strings is None or len(strings) == 0:
        return df_filtered
    includes_nan = "nan" in strings
    return df_filtered[df_filtered[column_name].isin(strings) | (includes_nan & pd.isna(df_filtered[column_name]))]
