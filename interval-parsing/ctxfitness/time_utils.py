import datetime as dt
from typing import Generator


def to_day_start_datetime(date: dt.datetime) -> dt.datetime:
    return dt.datetime.combine(date, dt.time())


def to_minute_beginning(datetime: dt.datetime) -> dt.datetime:
    return dt.datetime(datetime.year, datetime.month, datetime.day, datetime.hour, datetime.minute)


def parse_datestr_interval_time(date_str: str) -> dt.datetime:
    return dt.datetime.strptime(date_str, DATE_FORMAT)


DATE_FORMAT: str = '%Y-%m-%dT%H:%M:%S'


def gen_days_in_interval(start: dt.date, end: dt.date) -> Generator[dt.date, None, None]:
    for curr_date in (start + dt.timedelta(n) for n in range((end - start).days)):
        yield curr_date