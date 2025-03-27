from typing import List
import unittest
from ctxdashboard.figures.patient_heatmap import PreparedAcceptanceHeatMap, PreparedHeatmap
from ctxfitness.ctx_patient_dailies_handler import CTxPatientDailiesHandler, CTxPatientDay
import datetime as dt
import numpy as np

FIRST_PAT_ID = "FIRST_PAT_ID"
SECOND_PAT_ID = "SECOND_PAT_ID"

first_date = dt.date(2020, 12, 12)
second_date = dt.date(2020, 12, 13)
def true_criterion(x, y): return True
def false_criterion(x, y): return False


full_day_s = 60 * 60 * 24

dailies_handlers = [
    CTxPatientDailiesHandler(
        FIRST_PAT_ID,
        [
            CTxPatientDay.create_patient_day(
                day_date=first_date,
                duration_s=full_day_s,
                daily_criterion=true_criterion
            ),
            CTxPatientDay.create_patient_day(
                day_date=second_date,
                duration_s=full_day_s,
                daily_criterion=true_criterion
            )
        ], accepted=True
    ),
    CTxPatientDailiesHandler(
        SECOND_PAT_ID,
        [
            CTxPatientDay.create_patient_day(
                day_date=first_date,
                duration_s=0,
                daily_criterion=false_criterion
            )
        ], accepted=False
    )
]


class PatientHeatmapTest(unittest.TestCase):
    def test_prepare_heatmap_durations(self):
        prepared_heatmap: PreparedHeatmap = PreparedHeatmap.prepare_heatmap(
            dailies_handlers, 8)
        self.assertTrue(np.array_equal(
            prepared_heatmap.durations_matrix, np.array([[1, 1], [0, 0]])))

    def test_prepare_heatmap_hover(self):
        prepared_heatmap: PreparedHeatmap = PreparedHeatmap.prepare_heatmap(
            dailies_handlers, 8)
        self.assertTrue(np.array_equal(
            prepared_heatmap.hover_matrix, np.array([
                ["Day 1: 24:00:00", "Day 2: 24:00:00"],
                ["Day 1: 00:00:00", "Day 2: 00:00:00"]])))

    def test_prepare_heatmap_y_ticks(self):
        prepared_heatmap: PreparedHeatmap = PreparedHeatmap.prepare_heatmap(
            dailies_handlers, 8)
        self.assertCountEqual(
            prepared_heatmap.y_ticks,
            [f" {FIRST_PAT_ID} -",
             f" {SECOND_PAT_ID} -"]
        )

    def test_prepare_heatmap_x_ticks(self):
        prepared_heatmap: PreparedHeatmap = PreparedHeatmap.prepare_heatmap(
            dailies_handlers, 8)
        self.assertCountEqual(
            prepared_heatmap.x_ticks,
            ["Day 1",
             "Day 2"]
        )


class PreparedAcceptanceHeatMapTest(unittest.TestCase):
    def test_create_prepared_acceptance_matrix_acceptance_values(self):
        prepared_acceptance_heatmap: PreparedAcceptanceHeatMap = PreparedAcceptanceHeatMap.create_prepared_acceptance_matrix(
            dailies_handlers)
        self.assertTrue(
            np.array_equal(
                prepared_acceptance_heatmap.acceptance_values,
                [[1], [0]]
            )
        )

    def test_create_prepared_acceptance_matrix_hover_labels(self):
        prepared_acceptance_heatmap: PreparedAcceptanceHeatMap = PreparedAcceptanceHeatMap.create_prepared_acceptance_matrix(
            dailies_handlers)
        self.assertTrue(
            np.array_equal(
                prepared_acceptance_heatmap.hover_matrix,
                [["FIRST_PAT_ID accepted"], ["SECOND_PAT_ID not accepted"]]
            )
        )
