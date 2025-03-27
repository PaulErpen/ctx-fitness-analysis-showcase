from typing import Any, Callable, Dict, Generator, List
from ctxfitness.time_utils import to_day_start_datetime, to_minute_beginning
import pandas as pd
import datetime as dt
import logging

class Interval:
    start: dt.datetime
    end: dt.datetime
    data: "pd.Series[Any]"

    def __init__(self, start: dt.datetime, end: dt.datetime, data: "pd.Series[Any]") -> None:
        self.start = start
        self.end = end
        self.data = data

        if self.start > self.end:
            raise Exception("Error: interval cannot end before it starts")

    def get_delta(self) -> dt.timedelta:
        return self.end - self.start

    def get_days(self) -> List[dt.date]:
        return list(self.generate_days())

    def generate_days(self) -> Generator[dt.date, None, None]:
        delta_days = to_day_start_datetime(self.end.date()) - \
            to_day_start_datetime(self.start.date())
        for curr_date in (self.start.date() + dt.timedelta(n) for n in range(delta_days.days + 1)):
            yield curr_date

    def get_time_per_day(self) -> Dict[dt.date, dt.timedelta]:
        day_to_time: Dict[dt.date, dt.timedelta] = {}
        one_day = dt.timedelta(1)
        for day in self.generate_days():
            day_start = dt.datetime(day.year, day.month, day.day)
            interval_encloses_day = self.start <= day_start and day_start + one_day <= self.end
            interval_ends_in_day = self.start <= day_start and day_start < self.end and self.end <= day_start + one_day
            interval_begins_in_day = day_start <= self.start and day_start + one_day <= self.end
            interval_enclosed_by_day = day_start <= self.start and self.end <= day_start + one_day
            if interval_encloses_day:
                day_to_time[day] = one_day
            elif interval_ends_in_day:
                day_to_time[day] = self.end - day_start
            elif interval_begins_in_day:
                day_to_time[day] = day_start + one_day - self.start
            elif interval_enclosed_by_day:
                day_to_time[day] = self.get_delta()
        return day_to_time

    def generate_minutes(self) -> Generator[dt.datetime, None, None]:
        delta_minutes: dt.timedelta = to_minute_beginning(
            self.end) - to_minute_beginning(self.start)
        for curr_minute in (to_minute_beginning(self.start) + dt.timedelta(minutes=n) for n in range(int(delta_minutes.total_seconds() / 60) + 1)):
            yield curr_minute

    def get_minutes(self) -> List[dt.datetime]:
        return list(self.generate_minutes())

    def get_time_per_minute(self) -> Dict[dt.datetime, dt.timedelta]:
        min_to_time: Dict[dt.datetime, dt.timedelta] = {}
        one_min = dt.timedelta(minutes=1)
        for minute in self.generate_minutes():
            interval_encloses_minute = self.start <= minute and minute + one_min <= self.end
            interval_ends_in_minute = self.start <= minute and minute < self.end and self.end <= minute + one_min
            interval_begins_in_minute = minute <= self.start and minute + one_min <= self.end
            interval_enclosed_by_minute = minute <= self.start and self.end <= minute + one_min
            if interval_encloses_minute:
                min_to_time[minute] = one_min
            elif interval_ends_in_minute:
                min_to_time[minute] = self.end - minute
            elif interval_begins_in_minute:
                min_to_time[minute] = minute + one_min - self.start
            elif interval_enclosed_by_minute:
                min_to_time[minute] = self.get_delta()
        return min_to_time

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Interval):
            return other.start == self.start and other.end == self.end and self.data.equals(other.data)

        return False


class IntervalFraction:
    interval: Interval
    fraction: dt.timedelta

    def __init__(self, interval: Interval, fraction: dt.timedelta) -> None:
        self.interval = interval
        self.fraction = fraction


class IntervalNormalizer:
    intervals: List[Interval]

    def __init__(self, intervals: List[Interval]) -> None:
        IntervalNormalizer.log_interval_overlapping(intervals)
        self.intervals = intervals

    @staticmethod
    def log_interval_overlapping(intervals: List[Interval]):
        extract_start: Callable[[Interval], dt.datetime] = lambda i: i.start
        ints_sorted = sorted(intervals, key=extract_start)
        for i in range(len(ints_sorted) - 1):
            if ints_sorted[i].end > ints_sorted[i+1].start:
                logging.getLogger("IntervalNormalizer").error(
                    f"Intervals overlap! End of previous: '{ints_sorted[i].end}; Start of next: '{ints_sorted[i+1].start}'; Overlap in sec: {(ints_sorted[i].end - ints_sorted[i+1].start).total_seconds()}")
                logging.getLogger("IntervalNormalizer").error(f"First interval data: '{ints_sorted[i].data}'")
                logging.getLogger("IntervalNormalizer").error(f"Second interval data: '{ints_sorted[i+1].data}'")

    def normalize_by_day(self, aggregator: Callable[[List[IntervalFraction]], "pd.Series[Any]"]) -> List[Interval]:
        ints_per_day: Dict[dt.date, List[IntervalFraction]] = {}
        for interval in self.intervals:
            for day, time in interval.get_time_per_day().items():
                if day in ints_per_day:
                    ints_per_day[day].append(IntervalFraction(interval, time))
                else:
                    ints_per_day[day] = [IntervalFraction(interval, time)]

        aggregated_intervals: List[Interval] = []
        for day, interval_fractions in ints_per_day.items():
            aggregated_intervals.append(
                Interval(
                    to_day_start_datetime(day),
                    to_day_start_datetime(day) + dt.timedelta(1),
                    aggregator(interval_fractions)))

        return aggregated_intervals

    def normalize_by_minute(self, aggregator: Callable[[List[IntervalFraction]], "pd.Series[Any]"]) -> List[Interval]:
        ints_per_min: Dict[dt.datetime, List[IntervalFraction]] = {}
        for interval in self.intervals:
            for minute, time in interval.get_time_per_minute().items():
                if minute in ints_per_min:
                    ints_per_min[minute].append(
                        IntervalFraction(interval, time))
                else:
                    ints_per_min[minute] = [IntervalFraction(interval, time)]

        aggregated_intervals: List[Interval] = []
        for minute, interval_fractions in ints_per_min.items():
            aggregated_intervals.append(
                Interval(
                    to_minute_beginning(minute),
                    to_minute_beginning(minute) + dt.timedelta(minutes=1),
                    aggregator(interval_fractions)))

        return aggregated_intervals
