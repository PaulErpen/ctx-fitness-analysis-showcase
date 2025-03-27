from typing import Any, List
import unittest
import datetime as dt
import pandas as pd
from ctxfitness import interval_parser as ip


class IntervalTests(unittest.TestCase):
    some_start_time: dt.datetime = dt.datetime(2022, 12, 1, 1, 1, 1, 1)
    some_end_time: dt.datetime = dt.datetime(2022, 12, 2, 1, 1, 1, 1)
    some_interval: ip.Interval

    def setUp(self) -> None:
        self.some_interval = ip.Interval(self.some_start_time,
                                         self.some_end_time,
                                         pd.Series(dtype="float64"))

    def test_equality(self):
        self.assertEqual(
            ip.Interval(
                dt.datetime(2022, 12, 1, 1, 1, 1, 1),
                dt.datetime(2022, 12, 1, 2, 1, 1, 1),
                pd.Series([1, 2, 3])
            ),
            ip.Interval(
                dt.datetime(2022, 12, 1, 1, 1, 1, 1),
                dt.datetime(2022, 12, 1, 2, 1, 1, 1),
                pd.Series([1, 2, 3])
            )
        )

    def test_equality(self):
        self.assertNotEqual(
            ip.Interval(
                dt.datetime(2022, 12, 1, 1, 1, 1, 1),
                dt.datetime(2022, 12, 1, 2, 1, 1, 1),
                pd.Series([1, 2, 3])
            ),
            ip.Interval(
                dt.datetime(2022, 12, 1, 2, 1, 1, 1),
                dt.datetime(2022, 12, 1, 3, 1, 1, 1),
                pd.Series([1, 2, 3])
            )
        )

    def test_throw_exception_on_illegal_arguments(self):
        self.assertRaises(Exception, ip.Interval,
                          self.some_end_time, self.some_start_time, pd.Series(dtype="float64"))

    def test_get_delta(self):
        self.assertEqual(
            self.some_interval.get_delta(),
            dt.timedelta(1))

    def test_get_days_simple(self):
        self.assertEqual(self.some_interval.get_days(), [
                         dt.date(2022, 12, 1), dt.date(2022, 12, 2)])

    def test_get_days_complex(self):
        self.assertEqual(
            ip.Interval(self.some_start_time, dt.datetime(
                2022, 12, 2, 1, 0, 0, 0), pd.Series(dtype="float64")).get_days(),
            [dt.date(2022, 12, 1), dt.date(2022, 12, 2)])

    def test_get_time_per_day_all_cases(self):
        other_interval = ip.Interval(
            dt.datetime(2022, 12, 1, 1, 0, 0, 0),
            dt.datetime(2022, 12, 3, 1, 0, 0, 0),
            pd.Series(dtype="float64")
        )
        self.assertEqual(
            other_interval.get_time_per_day(),
            {
                dt.date(2022, 12, 1): dt.timedelta(hours=23),
                dt.date(2022, 12, 2): dt.timedelta(hours=24),
                dt.date(2022, 12, 3): dt.timedelta(hours=1)
            }
        )

    def test_get_time_per_day_two_cases(self):
        other_interval = ip.Interval(
            dt.datetime(2022, 12, 1, 1, 0, 0, 0),
            dt.datetime(2022, 12, 2, 1, 0, 0, 0),
            pd.Series(dtype="float64")
        )
        self.assertEqual(
            other_interval.get_time_per_day(),
            {
                dt.date(2022, 12, 1): dt.timedelta(hours=23),
                dt.date(2022, 12, 2): dt.timedelta(hours=1)
            }
        )

    def test_get_time_per_day_just_started(self):
        other_interval = ip.Interval(
            dt.datetime(2022, 12, 1, 1, 0, 0, 0),
            dt.datetime(2022, 12, 2, 0, 0, 1, 0),
            pd.Series(dtype="float64")
        )
        self.assertEqual(
            other_interval.get_time_per_day(),
            {
                dt.date(2022, 12, 1): dt.timedelta(hours=23),
                dt.date(2022, 12, 2): dt.timedelta(seconds=1)
            }
        )

    def test_get_time_per_day_only_started(self):
        other_interval = ip.Interval(
            dt.datetime(2022, 12, 1, 1, 0, 0, 0),
            dt.datetime(2022, 12, 2, 0, 0, 0, 0),
            pd.Series(dtype="float64")
        )
        self.assertEqual(
            other_interval.get_time_per_day(),
            {
                dt.date(2022, 12, 1): dt.timedelta(hours=23)
            }
        )

    def test_get_time_per_day_only_ended(self):
        other_interval = ip.Interval(
            dt.datetime(2022, 12, 1, 0, 0, 0, 0),
            dt.datetime(2022, 12, 1, 2, 0, 0, 0),
            pd.Series(dtype="float64")
        )
        self.assertEqual(
            other_interval.get_time_per_day(),
            {
                dt.date(2022, 12, 1): dt.timedelta(hours=2)
            }
        )

    def test_get_minutes(self):
        small_interval = ip.Interval(
            dt.datetime(2022, 12, 1, 0, 0, 0, 0),
            dt.datetime(2022, 12, 1, 0, 2, 0, 0),
            pd.Series(dtype="float64")
        )
        self.assertEqual(
            small_interval.get_minutes(),
            [dt.datetime(2022, 12, 1, 0, 0, 0, 0),
             dt.datetime(2022, 12, 1, 0, 1, 0, 0),
             dt.datetime(2022, 12, 1, 0, 2, 0, 0)]
        )

    def test_get_time_per_minute_all_cases(self):
        other_interval = ip.Interval(
            dt.datetime(2022, 12, 1, 0, 0, 30, 0),
            dt.datetime(2022, 12, 1, 0, 2, 30, 0),
            pd.Series(dtype="float64")
        )
        self.assertEqual(
            other_interval.get_time_per_minute(),
            {
                dt.datetime(2022, 12, 1, 0, 0): dt.timedelta(seconds=30),
                dt.datetime(2022, 12, 1, 0, 1): dt.timedelta(seconds=60),
                dt.datetime(2022, 12, 1, 0, 2): dt.timedelta(seconds=30)
            }
        )

    def test_get_time_per_minute_two_cases(self):
        other_interval = ip.Interval(
            dt.datetime(2022, 12, 1, 0, 0, 30, 0),
            dt.datetime(2022, 12, 1, 0, 1, 30, 0),
            pd.Series(dtype="float64")
        )
        self.assertEqual(
            other_interval.get_time_per_minute(),
            {
                dt.datetime(2022, 12, 1, 0, 0): dt.timedelta(seconds=30),
                dt.datetime(2022, 12, 1, 0, 1): dt.timedelta(seconds=30)
            }
        )

    def test_get_time_per_minute_only_started(self):
        other_interval = ip.Interval(
            dt.datetime(2022, 12, 1, 0, 0, 30, 0),
            dt.datetime(2022, 12, 1, 0, 1, 0, 0),
            pd.Series(dtype="float64")
        )
        self.assertEqual(
            other_interval.get_time_per_minute(),
            {
                dt.datetime(2022, 12, 1, 0, 0): dt.timedelta(seconds=30)
            }
        )

    def test_get_time_per_minute_only_ended(self):
        other_interval = ip.Interval(
            dt.datetime(2022, 12, 1, 0, 1, 0, 0),
            dt.datetime(2022, 12, 1, 0, 1, 30, 0),
            pd.Series(dtype="float64")
        )
        self.assertEqual(
            other_interval.get_time_per_minute(),
            {
                dt.datetime(2022, 12, 1, 0, 1): dt.timedelta(seconds=30)
            }
        )


def simple_test_sum_aggregator(interval_fractions: List[ip.IntervalFraction]) -> "pd.Series[Any]":
    data: List[pd.Series["Any"]] = list(
        map(lambda i_f: i_f.interval.data, interval_fractions))
    return pd.concat(data, axis=1).transpose().sum()


class IntervalNormalizerTests(unittest.TestCase):
    some_index: List[str] = ["one", "two", "three"]
    maxDiff = None

    def test_daily_interval_aggregation(self):
        i_one = ip.Interval(
            dt.datetime(2022, 12, 1, 1, 30, 0, 0),
            dt.datetime(2022, 12, 1, 3, 30, 0, 0),
            pd.Series(data=[2, 3, 4], index=self.some_index)
        )
        i_two = ip.Interval(
            dt.datetime(2022, 12, 1, 3, 40, 0, 0),
            dt.datetime(2022, 12, 1, 5, 30, 0, 0),
            pd.Series(data=[2, 3, 4], index=self.some_index)
        )
        i_three = ip.Interval(
            dt.datetime(2022, 12, 1, 6, 40, 0, 0),
            dt.datetime(2022, 12, 2, 5, 30, 0, 0),
            pd.Series(data=[2, 3, 4], index=self.some_index)
        )
        i_four = ip.Interval(
            dt.datetime(2022, 12, 3, 3, 40, 0, 0),
            dt.datetime(2022, 12, 5, 0, 0, 0, 0),
            pd.Series(data=[2, 3, 4], index=self.some_index)
        )
        normalizer: ip.IntervalNormalizer = ip.IntervalNormalizer(
            intervals=[i_one, i_two, i_three, i_four])
        aggregated_intervals: List[ip.Interval] = normalizer.normalize_by_day(
            simple_test_sum_aggregator)
        self.assertSequenceEqual(
            aggregated_intervals,
            [
                ip.Interval(
                    dt.datetime(2022, 12, 1, 0, 0, 0, 0),
                    dt.datetime(2022, 12, 2, 0, 0, 0, 0),
                    pd.Series([6, 9, 12], index=self.some_index)
                ),
                ip.Interval(
                    dt.datetime(2022, 12, 2, 0, 0, 0, 0),
                    dt.datetime(2022, 12, 3, 0, 0, 0, 0),
                    pd.Series([2, 3, 4], index=self.some_index)
                ),
                ip.Interval(
                    dt.datetime(2022, 12, 3, 0, 0, 0, 0),
                    dt.datetime(2022, 12, 4, 0, 0, 0, 0),
                    pd.Series([2, 3, 4], index=self.some_index)
                ),
                ip.Interval(
                    dt.datetime(2022, 12, 4, 0, 0, 0, 0),
                    dt.datetime(2022, 12, 5, 0, 0, 0, 0),
                    pd.Series([2, 3, 4], index=self.some_index)
                )
            ]
        )

    def test_minutely_interval_aggregation(self):
        i_one = ip.Interval(
            dt.datetime(2022, 12, 1, 0, 1, 0, 0),
            dt.datetime(2022, 12, 1, 0, 2, 0, 0),
            pd.Series(data=[2, 3, 4], index=self.some_index)
        )
        i_two = ip.Interval(
            dt.datetime(2022, 12, 1, 0, 2, 30, 0),
            dt.datetime(2022, 12, 1, 0, 3, 30, 0),
            pd.Series(data=[2, 3, 4], index=self.some_index)
        )
        i_three = ip.Interval(
            dt.datetime(2022, 12, 1, 0, 3, 45, 0),
            dt.datetime(2022, 12, 1, 0, 4, 0, 0),
            pd.Series(data=[2, 3, 4], index=self.some_index)
        )
        i_four = ip.Interval(
            dt.datetime(2022, 12, 1, 0, 5, 30, 0),
            dt.datetime(2022, 12, 1, 0, 6, 30, 0),
            pd.Series(data=[2, 3, 4], index=self.some_index)
        )
        normalizer: ip.IntervalNormalizer = ip.IntervalNormalizer(
            intervals=[i_one, i_two, i_three, i_four])
        aggregated_intervals: List[ip.Interval] = normalizer.normalize_by_minute(
            simple_test_sum_aggregator)
        self.assertSequenceEqual(
            aggregated_intervals,
            [
                ip.Interval(
                    dt.datetime(2022, 12, 1, 0, 1, 0, 0),
                    dt.datetime(2022, 12, 1, 0, 2, 0, 0),
                    pd.Series([2, 3, 4], index=self.some_index)
                ),
                ip.Interval(
                    dt.datetime(2022, 12, 1, 0, 2, 0, 0),
                    dt.datetime(2022, 12, 1, 0, 3, 0, 0),
                    pd.Series([2, 3, 4], index=self.some_index)
                ),
                ip.Interval(
                    dt.datetime(2022, 12, 1, 0, 3, 0, 0),
                    dt.datetime(2022, 12, 1, 0, 4, 0, 0),
                    pd.Series([4, 6, 8], index=self.some_index)
                ),
                ip.Interval(
                    dt.datetime(2022, 12, 1, 0, 5, 0, 0),
                    dt.datetime(2022, 12, 1, 0, 6, 0, 0),
                    pd.Series([2, 3, 4], index=self.some_index)
                ),
                ip.Interval(
                    dt.datetime(2022, 12, 1, 0, 6, 0, 0),
                    dt.datetime(2022, 12, 1, 0, 7, 0, 0),
                    pd.Series([2, 3, 4], index=self.some_index)
                )
            ]
        )

    def test_throw_exception_on_overlapping_intervals(self):
        with self.assertLogs("IntervalNormalizer", level="ERROR") as logs:
            ip.IntervalNormalizer([
                ip.Interval(
                    dt.datetime(2022, 12, 1, 0, 2, 0, 0),
                    dt.datetime(2022, 12, 1, 0, 6, 0, 0),
                    pd.Series()
                ),
                ip.Interval(
                    dt.datetime(2022, 12, 1, 0, 11, 0, 0),
                    dt.datetime(2022, 12, 1, 0, 22, 0, 0),
                    pd.Series()
                ),
                ip.Interval(
                    dt.datetime(2022, 12, 2, 0, 11, 0, 0),
                    dt.datetime(2022, 12, 3, 0, 22, 0, 0),
                    pd.Series()
                ),
                ip.Interval(
                    dt.datetime(2022, 12, 3, 0, 22, 0, 0),
                    dt.datetime(2022, 12, 4, 0, 22, 0, 0),
                    pd.Series()
                ),
                ip.Interval(
                    dt.datetime(2022, 12, 1, 0, 10, 0, 0),
                    dt.datetime(2022, 12, 2, 0, 22, 0, 0),
                    pd.Series()
                )
            ])
            self.assertEquals(logs.output, [
                "ERROR:IntervalNormalizer:Intervals overlap! End of previous: '2022-12-02 00:22:00; Start of next: '2022-12-01 00:11:00'; Overlap in sec: 87060.0"
            ])


if __name__ == '__main__':
    unittest.main()
