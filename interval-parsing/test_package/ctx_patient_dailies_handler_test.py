from typing import Callable, List
import unittest
import pandas as pd
import ctxfitness.preprocessing_pipeline as ipc
from ctxfitness.ctx_patient_dailies_handler import CTxPatientDailiesHandler, CTxPatientDay, minimum_overall_and_consecutive_days_patient_acceptance_criterion, simple_min_hours_daily_acceptance_criterion
import datetime as dt
from pyxtension.streams import stream

simple_daily_criterion: Callable[[
    dt.date, int], bool] = lambda day, sec: True if sec > 60 * 60 * 8 else False

simple_patient_criterion: Callable[[List[CTxPatientDay]], bool] = lambda days: \
    len(stream(days)  # type: ignore
        .filter(lambda day: day.accepted)
        .toList()) > 2

SOME_PAT_ID = "SOME_PAT_ID"


class CTxPatientDailiesHandlerTest(unittest.TestCase):
    df: pd.DataFrame

    @classmethod
    def setUpClass(cls):
        cls.df = ipc.interval_parse_dailies(
            pd.read_excel("../SampleData/Dailies 012.xlsx"))

    def test_from_frame(self):
        dailies_handler: CTxPatientDailiesHandler = CTxPatientDailiesHandler.from_frame(
            SOME_PAT_ID,
            self.df.iloc[[0, 1]],
            daily_criterion=simple_daily_criterion,
            patient_acceptance_criterion=simple_patient_criterion)
        self.assertEqual(
            dailies_handler,
            CTxPatientDailiesHandler(
                SOME_PAT_ID,
                [
                    CTxPatientDay(dt.date(2021, 8, 1),
                                  86400, True, "24:00:00"),
                    CTxPatientDay(dt.date(2021, 8, 2), 86400, True, "24:00:00")
                ],
                accepted=False
            )
        )

    def test_from_frame_accepted(self):
        dailies_handler: CTxPatientDailiesHandler = CTxPatientDailiesHandler.from_frame(
            SOME_PAT_ID,
            self.df.iloc[[0, 1, 2]],
            daily_criterion=simple_daily_criterion,
            patient_acceptance_criterion=simple_patient_criterion
        )
        self.assertEqual(
            dailies_handler,
            CTxPatientDailiesHandler(
                SOME_PAT_ID,
                [
                    CTxPatientDay(dt.date(2021, 8, 1),
                                  86400, True, "24:00:00"),
                    CTxPatientDay(dt.date(2021, 8, 2),
                                  86400, True, "24:00:00"),
                    CTxPatientDay(dt.date(2021, 8, 3), 86400, True, "24:00:00")
                ],
                True
            )
        )

    def test_accept_day_test_to_small(self):
        self.assertFalse(
            simple_min_hours_daily_acceptance_criterion(
                12)(dt.date(2022, 12, 12), 60 * 60 * 12 - 1)
        )

    def test_accept_day_test_just_true(self):
        self.assertTrue(
            simple_min_hours_daily_acceptance_criterion(
                12)(dt.date(2022, 12, 12), 60 * 60 * 12)
        )

    def test_accept_day_test_really_true(self):
        self.assertTrue(
            simple_min_hours_daily_acceptance_criterion(
                12)(dt.date(2022, 12, 12), 60 * 60 * 13)
        )

    def test_simple_patient_criterion_to_little(self):
        self.assertFalse(
            simple_patient_criterion([])
        )

    def test_simple_patient_criterion_accepted(self):
        self.assertTrue(
            simple_patient_criterion([CTxPatientDay(dt.date(2021, 8, 1),
                                                    86400, True, "24:00:00"),
                                      CTxPatientDay(dt.date(2021, 8, 2),
                                                    86400, True, "24:00:00"),
                                      CTxPatientDay(dt.date(2021, 8, 3), 86400, True, "24:00:00")])
        )

    def test_get_sum_accepted_days(self):
        handler = CTxPatientDailiesHandler(
            SOME_PAT_ID,
            [CTxPatientDay(dt.date(2021, 8, 1),
                           86400, True, "24:00:00"),
             CTxPatientDay(dt.date(2021, 8, 2),
                           86400, True, "24:00:00"),
             CTxPatientDay(dt.date(2021, 8, 3), 86400, False, "24:00:00")],
            accepted=True)
        self.assertEquals(handler.get_sum_accepted_days(), 2)

    def test_minimum_overall_and_consecutive_days_patient_acceptance_criterion_accept_seq(self):
        def some_day(accepted: bool) -> CTxPatientDay:
            return CTxPatientDay(dt.date(2021, 8, 1), 0, accepted, "")
        criterion = minimum_overall_and_consecutive_days_patient_acceptance_criterion(
            1, 3)
        self.assertTrue(
            criterion(
                [some_day(False),
                 some_day(True),
                 some_day(True),
                 some_day(True),
                 some_day(False),
                 some_day(True),
                 some_day(True)]
            ))
    
    def test_minimum_overall_and_consecutive_days_patient_acceptance_criterion_dont_accept_seq(self):
        def some_day(accepted: bool) -> CTxPatientDay:
            return CTxPatientDay(dt.date(2021, 8, 1), 0, accepted, "")
        criterion = minimum_overall_and_consecutive_days_patient_acceptance_criterion(
            100, 3)
        self.assertFalse(
            criterion(
                [some_day(False),
                 some_day(True),
                 some_day(True),
                 some_day(False),
                 some_day(False),
                 some_day(True),
                 some_day(True)]
            ))
        
    def test_minimum_overall_and_consecutive_days_patient_acceptance_criterion_accept_min_days(self):
        def some_day(accepted: bool) -> CTxPatientDay:
            return CTxPatientDay(dt.date(2021, 8, 1), 0, accepted, "")
        criterion = minimum_overall_and_consecutive_days_patient_acceptance_criterion(
            4, 1)
        self.assertTrue(
            criterion(
                [some_day(False),
                 some_day(True),
                 some_day(True),
                 some_day(False),
                 some_day(False),
                 some_day(True),
                 some_day(True)]
            ))
    
    def test_minimum_overall_and_consecutive_days_patient_acceptance_criterion_dont_accept_min_days(self):
        def some_day(accepted: bool) -> CTxPatientDay:
            return CTxPatientDay(dt.date(2021, 8, 1), 0, accepted, "")
        criterion = minimum_overall_and_consecutive_days_patient_acceptance_criterion(
            5, 3)
        self.assertFalse(
            criterion(
                [some_day(False),
                 some_day(True),
                 some_day(True),
                 some_day(False),
                 some_day(False),
                 some_day(True),
                 some_day(True)]
            ))


class CTxPatientDayTest(unittest.TestCase):
    def test_format_seconds(self):
        self.assertEqual(
            CTxPatientDay.format_daily_seconds(12),
            "00:00:12"
        )

    def test_format_seconds_day(self):
        self.assertEqual(
            CTxPatientDay.format_daily_seconds(60 * 60 * 24),
            "24:00:00"
        )

    def test_format_seconds_halfday(self):
        self.assertEqual(
            CTxPatientDay.format_daily_seconds(60 * 60 * 12),
            "12:00:00"
        )

    def test_format_seconds_halfday_and_half_hour(self):
        self.assertEqual(
            CTxPatientDay.format_daily_seconds(60 * 60 * 12 + 30 * 60),
            "12:30:00"
        )

    def test_format_seconds_halfday_and_half_hour_and_fifteen_sec(self):
        self.assertEqual(
            CTxPatientDay.format_daily_seconds(60 * 60 * 12 + 30 * 60 + 15),
            "12:30:15"
        )
