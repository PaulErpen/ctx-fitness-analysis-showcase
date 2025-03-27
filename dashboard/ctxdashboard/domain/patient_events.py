from dataclasses import dataclass
from typing import Dict, Generator
from ctxdashboard.util.listable_string_enum import ListableStringEnum
import pandas as pd
from ctxfitness.preprocessing_pipeline import ParsedDailiesColumns as pdc
from ctxfitness.time_utils import gen_days_in_interval
import numpy as np


class PatientEventTypes(ListableStringEnum):
    SCHEDULED_THERAPY_NOT_RECEIVED = "scheduled therapy not received (any cause)"
    SCHEDULED_THERAPY_RECEIVED = "scheduled therapy received"
    HOPSPITALIZED = "hospitalized (any cause)"
    SYSTEMIC_INFECTION_HOSP = "systemic infection (hospitalized) / antibiotics administered"
    SYSTEMIC_INFECTION_NON_HOSP = "systemic infection (non-hospitalized) (@Anna: oder 'antibiotics prescribed')"
    NEUTROPENIC_FEVER = "neutropenic fever"
    THERAPY_SWITCH = "therapy switch (any cause)"
    THERAPY_STOP = "therapy stop (any cause)"
    AE_I = "AE >I° (any event)"
    RECIST = "RECIST response (CR | PR | PD | SD | MR)"
    EMERGENCY_VISIT = "visit to emergency department"
    DEATH = "death"


@dataclass
class PatientEventSheetBundle:
    tracker_id: int
    sheet: pd.DataFrame


def create_patient_events_sheets(dailies: pd.DataFrame) -> Generator[PatientEventSheetBundle, None, None]:
    tracker_ids = dailies[pdc.USER_LAST_NAME].unique()
    first_day_overall: pd.Timestamp = dailies[pdc.START_DT].min()
    last_day_overall: pd.Timestamp = dailies[pdc.START_DT].max()

    all_days = list(gen_days_in_interval(first_day_overall.to_pydatetime(
    ).date(), last_day_overall.to_pydatetime().date()))

    for tracker_id in tracker_ids:
        daily_durations: Dict[str, int] = {kv[0]: kv[1] for kv in
                                           dailies[dailies[pdc.USER_LAST_NAME]
                                                   == tracker_id]
                                           .apply(lambda r: (r[pdc.START_DT].date(), r[pdc.DAILY_DURATION_S]), axis=1)}
        df_track = pd.DataFrame(
            data={
                "tracker_id": np.repeat(tracker_id, len(all_days)),
                "day": all_days,
                "time_worn_s": [daily_durations[d] if d in daily_durations else 0 for d in all_days]
            }
        )
        for event_type in PatientEventTypes.values():
            df_track[event_type] = np.nan

        yield PatientEventSheetBundle(tracker_id, df_track)
