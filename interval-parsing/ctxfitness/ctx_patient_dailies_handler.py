from dataclasses import dataclass
import datetime as dt
from typing import Callable, List
from ctxfitness.time_utils import gen_days_in_interval
import pandas as pd
from pyxtension.streams import stream
from ctxfitness.preprocessing_pipeline import ParsedDailiesColumns as pdc

SECONDS_IN_A_DAY = 60 * 60 * 24


class CTxPatientDay:
    ZERO_TIME_STRING = "00:00:00"

    def __init__(self, day_date: dt.date,
                 duration_s: int,
                 accepted: bool,
                 formatted_duration: str) -> None:
        self.day_date = day_date
        self.duration_s = duration_s
        self.accepted = accepted
        self.formatted_duration = formatted_duration

    @classmethod
    def create_patient_day(
        cls,
        day_date: dt.date,
        duration_s: int,
        daily_criterion: Callable[[dt.date, int], bool],
    ) -> "CTxPatientDay":
        return cls(
            day_date=day_date,
            duration_s=duration_s,
            accepted=daily_criterion(day_date, duration_s),
            formatted_duration=cls.format_daily_seconds(duration_s))

    @staticmethod
    def format_daily_seconds(duration_s: int) -> str:
        hours = duration_s // 3600
        minutes = duration_s // 60 - hours * 60
        seconds = duration_s - hours * 60 * 60 - minutes * 60
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, CTxPatientDay):
            return (
                self.day_date == __o.day_date and
                self.duration_s == __o.duration_s and
                self.accepted == __o.accepted and
                self.formatted_duration == __o.formatted_duration
            )
        else:
            return False


class CTxPatientDailiesHandler():

    def __init__(self, patient_id: str, patient_days: List[CTxPatientDay], accepted: bool) -> None:
        self.patient_id = patient_id
        self.patient_days = patient_days
        self.accepted = accepted

    @classmethod
    def from_frame(
        cls,
        patient_id: str,
        df: pd.DataFrame,
        daily_criterion: Callable[[dt.date, int], bool],
        patient_acceptance_criterion: Callable[[List[CTxPatientDay]], bool]
    ):
        start_date = df[pdc.START_DT].min().to_pydatetime().date()
        end_date = df[pdc.END_DT].max().to_pydatetime().date()
        existing_durations = dict([
            (r[pdc.START_DT].to_pydatetime().date(), r[pdc.DAILY_DURATION_S]) for l, r in df.iterrows()
        ])
        patient_days: List[CTxPatientDay] = []
        for day in gen_days_in_interval(start_date, end_date):
            duration: float = 0 if day not in existing_durations else existing_durations[day]
            patient_days.append(CTxPatientDay.create_patient_day(
                day_date=day,
                duration_s=int(duration),
                daily_criterion=daily_criterion
            ))
        return cls(patient_id, patient_days, patient_acceptance_criterion(patient_days))

    def get_days(self) -> List[dt.date]:
        return [d.day_date for d in self.patient_days]

    def get_durations(self) -> List[int]:
        return [d.duration_s for d in self.patient_days]

    def get_accepted(self) -> List[bool]:
        return [d.accepted for d in self.patient_days]

    def get_formatted_durations(self) -> List[str]:
        return [d.formatted_duration for d in self.patient_days]

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, CTxPatientDailiesHandler):
            return (
                self.patient_days == __o.patient_days
            )
        return False

    def get_sum_accepted_days(self) -> int:
        return sum(self.get_accepted())


N_SECS_HOUR = 60 * 60


def simple_min_hours_daily_acceptance_criterion(min_hours: int) -> Callable[[dt.date, int], bool]:
    def accept_day(_day: dt.date, sec: int) -> bool:
        return sec >= min_hours * N_SECS_HOUR

    return accept_day


def simple_min_days_patient_acceptance_criterion(min_days: int) -> Callable[[List[CTxPatientDay]], bool]:
    def accept_patient(days: List[CTxPatientDay]) -> bool:
        return len(stream(days)  # type: ignore
                   .filter(lambda day: day.accepted)
                   .toList()) >= min_days

    return accept_patient


def minimum_overall_and_consecutive_days_patient_acceptance_criterion(min_days: int, min_consecutive_days: int) -> Callable[[List[CTxPatientDay]], bool]:
    def accept_patient(days: List[CTxPatientDay]) -> bool:
        min_days_criterion = len(stream(days)  # type: ignore
                                 .filter(lambda day: day.accepted)
                                 .toList()) >= min_days

        sequences = []
        prev = None
        for day in days:
            if day.accepted:
                if prev is None or prev.accepted is False:
                    sequences.append(1)
                else:
                    sequences[-1] = sequences[-1] + 1
            prev = day
        consecutive_days_criterion = len(sequences) > 0 and max(
            sequences) >= min_consecutive_days
        return min_days_criterion and consecutive_days_criterion

    return accept_patient
