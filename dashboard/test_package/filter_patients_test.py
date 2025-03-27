import unittest
import pandas as pd
from ctxdashboard.domain.patientmeta import PatientMetaColumn as pmc
from ctxdashboard.filter_patients.filter_patients import filter_patients
import numpy as np


class FilterPatientsTest(unittest.TestCase):
    def setUp(self):
        self.df: pd.DataFrame = pd.DataFrame({
            pmc.TRACKER_ID: [1, 2, 3, 4, 5],
            pmc.AGE: [80.1, 91.1, 45.2, 83.01, 12.23],
            pmc.GENDER: ["F", "M", "F", "F", "M"],
            pmc.ECOG: [1.0, 0.0, 2.0, 1.0, np.NaN],
            pmc.THERAPY: [
                np.NaN,
                "FOLFOX 6x nach RAPIDO-Like Schema",
                "Epirubicin, Ifosfamid",
                "FOLFOX + Nivo",
                "FOLFIRINOX"
            ],
            pmc.THERAPY_REGIMEN: [
                "Chemotherapy",
                "Chemoimmunotherapy",
                "Chemotherapy",
                "Chemoimmunotherapy",
                "Chemo + Targeted",
            ],
            pmc.TREATMENT_NAIVE: [
                "Yes",
                "Yes",
                "Yes",
                "No",
                "No",
            ],
            pmc.PRIOR_TREATMENT: [
                "Chemo + Targeted",
                "Chemotherapy",
                "none",
                "none",
                "Immunotherapy",
            ],
            pmc.PRIMARY_TUMOR: [
                "Gastric",
                "Lung",
                "Lung",
                "Sarcoma",
                "Sarcoma",
            ],
            pmc.TUG: [
                14.0,
                7.0,
                np.NaN,
                np.NaN,
                8.0,
            ],
            pmc.HGS: [
                np.NaN,
                39.0,
                35.0,
                32.0,
                25.0,
            ]
        })

    def test_filter_by_ecogs_include_nan(self):
        result_df = filter_patients(
            patient_cofactors=self.df,
            ecog_values=["1.0", "nan"],
            age_interval=None,
            gender_multi_select_values=None,
            therapy_multi_select=None,
            therapy_regimen_multi_select=None,
            treatment_naive_multi_select=None,
            prior_treatment_multi_select=None,
            primary_tumor_multi_select=None,
            tug_range_slider=None,
            tug_include_nans=None,
            hgs_range_slider=None,
            hgs_include_nans=None,
        )
        self.assertEqual(
            len(result_df),
            3
        )

    def test_filter_by_ecogs_exclude_nan(self):
        result_df = filter_patients(
            patient_cofactors=self.df,
            ecog_values=["1.0"],
            age_interval=None,
            gender_multi_select_values=None,
            therapy_multi_select=None,
            therapy_regimen_multi_select=None,
            treatment_naive_multi_select=None,
            prior_treatment_multi_select=None,
            primary_tumor_multi_select=None,
            tug_range_slider=None,
            tug_include_nans=None,
            hgs_range_slider=None,
            hgs_include_nans=None,
        )
        self.assertEqual(
            len(result_df),
            2
        )

    def test_filter_by_ecogs_none_selected(self):
        result_df = filter_patients(
            patient_cofactors=self.df,
            ecog_values=[],
            age_interval=None,
            gender_multi_select_values=None,
            therapy_multi_select=None,
            therapy_regimen_multi_select=None,
            treatment_naive_multi_select=None,
            prior_treatment_multi_select=None,
            primary_tumor_multi_select=None,
            tug_range_slider=None,
            tug_include_nans=None,
            hgs_range_slider=None,
            hgs_include_nans=None,
        )
        self.assertEqual(
            len(result_df),
            5
        )

    def test_filter_by_age(self):
        result_df = filter_patients(
            patient_cofactors=self.df,
            ecog_values=None,
            age_interval=("20.23", "85.2"),
            gender_multi_select_values=None,
            therapy_multi_select=None,
            therapy_regimen_multi_select=None,
            treatment_naive_multi_select=None,
            prior_treatment_multi_select=None,
            primary_tumor_multi_select=None,
            tug_range_slider=None,
            tug_include_nans=None,
            hgs_range_slider=None,
            hgs_include_nans=None,
        )
        self.assertEqual(
            len(result_df),
            3
        )

    def test_filter_by_gender(self):
        result_df = filter_patients(
            patient_cofactors=self.df,
            ecog_values=None,
            age_interval=None,
            gender_multi_select_values=["F"],
            therapy_multi_select=None,
            therapy_regimen_multi_select=None,
            treatment_naive_multi_select=None,
            prior_treatment_multi_select=None,
            primary_tumor_multi_select=None,
            tug_range_slider=None,
            tug_include_nans=None,
            hgs_range_slider=None,
            hgs_include_nans=None,
        )
        self.assertEqual(
            len(result_df),
            3
        )

    def test_filter_by_gender_none_selected(self):
        result_df = filter_patients(
            patient_cofactors=self.df,
            ecog_values=None,
            age_interval=None,
            gender_multi_select_values=[],
            therapy_multi_select=None,
            therapy_regimen_multi_select=None,
            treatment_naive_multi_select=None,
            prior_treatment_multi_select=None,
            primary_tumor_multi_select=None,
            tug_range_slider=None,
            tug_include_nans=None,
            hgs_range_slider=None,
            hgs_include_nans=None,
        )
        self.assertEqual(
            len(result_df),
            5
        )

    def test_filter_by_therapy_include_nans(self):
        result_df = filter_patients(
            patient_cofactors=self.df,
            ecog_values=None,
            age_interval=None,
            gender_multi_select_values=None,
            therapy_multi_select=["nan",
                                  "FOLFOX + Nivo",
                                  "FOLFIRINOX"],
            therapy_regimen_multi_select=None,
            treatment_naive_multi_select=None,
            prior_treatment_multi_select=None,
            primary_tumor_multi_select=None,
            tug_range_slider=None,
            tug_include_nans=None,
            hgs_range_slider=None,
            hgs_include_nans=None,
        )
        self.assertEqual(
            len(result_df),
            3
        )

    def test_filter_by_therapy_exclude_nans(self):
        result_df = filter_patients(
            patient_cofactors=self.df,
            ecog_values=None,
            age_interval=None,
            gender_multi_select_values=None,
            therapy_multi_select=["FOLFOX + Nivo",
                                  "FOLFIRINOX"],
            therapy_regimen_multi_select=None,
            treatment_naive_multi_select=None,
            prior_treatment_multi_select=None,
            primary_tumor_multi_select=None,
            tug_range_slider=None,
            tug_include_nans=None,
            hgs_range_slider=None,
            hgs_include_nans=None,
        )
        self.assertEqual(
            len(result_df),
            2
        )

    def test_filter_by_therapy_none_selected(self):
        result_df = filter_patients(
            patient_cofactors=self.df,
            ecog_values=None,
            age_interval=None,
            gender_multi_select_values=None,
            therapy_multi_select=[],
            therapy_regimen_multi_select=None,
            treatment_naive_multi_select=None,
            prior_treatment_multi_select=None,
            primary_tumor_multi_select=None,
            tug_range_slider=None,
            tug_include_nans=None,
            hgs_range_slider=None,
            hgs_include_nans=None,
        )
        self.assertEqual(
            len(result_df),
            5
        )

    def test_filter_by_therapy_regimen(self):
        result_df = filter_patients(
            patient_cofactors=self.df,
            ecog_values=None,
            age_interval=None,
            gender_multi_select_values=None,
            therapy_multi_select=None,
            therapy_regimen_multi_select=[
                "Chemotherapy",
                "Chemoimmunotherapy"
            ],
            treatment_naive_multi_select=None,
            prior_treatment_multi_select=None,
            primary_tumor_multi_select=None,
            tug_range_slider=None,
            tug_include_nans=None,
            hgs_range_slider=None,
            hgs_include_nans=None,
        )
        self.assertEqual(
            len(result_df),
            4
        )

    def test_filter_by_therapy_regimen_none_selected(self):
        result_df = filter_patients(
            patient_cofactors=self.df,
            ecog_values=None,
            age_interval=None,
            gender_multi_select_values=None,
            therapy_multi_select=None,
            therapy_regimen_multi_select=[],
            treatment_naive_multi_select=None,
            prior_treatment_multi_select=None,
            primary_tumor_multi_select=None,
            tug_range_slider=None,
            tug_include_nans=None,
            hgs_range_slider=None,
            hgs_include_nans=None,
        )
        self.assertEqual(
            len(result_df),
            5
        )

    def test_filter_by_therapy_naive(self):
        result_df = filter_patients(
            patient_cofactors=self.df,
            ecog_values=None,
            age_interval=None,
            gender_multi_select_values=None,
            therapy_multi_select=None,
            therapy_regimen_multi_select=None,
            treatment_naive_multi_select=["Yes"],
            prior_treatment_multi_select=None,
            primary_tumor_multi_select=None,
            tug_range_slider=None,
            tug_include_nans=None,
            hgs_range_slider=None,
            hgs_include_nans=None,
        )
        self.assertEqual(
            len(result_df),
            3
        )

    def test_filter_by_therapy_naive_none_selected(self):
        result_df = filter_patients(
            patient_cofactors=self.df,
            ecog_values=None,
            age_interval=None,
            gender_multi_select_values=None,
            therapy_multi_select=None,
            therapy_regimen_multi_select=None,
            treatment_naive_multi_select=[],
            prior_treatment_multi_select=None,
            primary_tumor_multi_select=None,
            tug_range_slider=None,
            tug_include_nans=None,
            hgs_range_slider=None,
            hgs_include_nans=None,
        )
        self.assertEqual(
            len(result_df),
            5
        )

    def test_filter_by_prior_treatment(self):
        result_df = filter_patients(
            patient_cofactors=self.df,
            ecog_values=None,
            age_interval=None,
            gender_multi_select_values=None,
            therapy_multi_select=None,
            therapy_regimen_multi_select=None,
            treatment_naive_multi_select=None,
            prior_treatment_multi_select=[
                "Chemo + Targeted",
                "Chemotherapy",
            ],
            primary_tumor_multi_select=None,
            tug_range_slider=None,
            tug_include_nans=None,
            hgs_range_slider=None,
            hgs_include_nans=None,
        )
        self.assertEqual(
            len(result_df),
            2
        )

    def test_filter_by_prior_treatment_none_selected(self):
        result_df = filter_patients(
            patient_cofactors=self.df,
            ecog_values=None,
            age_interval=None,
            gender_multi_select_values=None,
            therapy_multi_select=None,
            therapy_regimen_multi_select=None,
            treatment_naive_multi_select=None,
            prior_treatment_multi_select=[],
            primary_tumor_multi_select=None,
            tug_range_slider=None,
            tug_include_nans=None,
            hgs_range_slider=None,
            hgs_include_nans=None,
        )
        self.assertEqual(
            len(result_df),
            5
        )

    def test_filter_by_primary_tumor(self):
        result_df = filter_patients(
            patient_cofactors=self.df,
            ecog_values=None,
            age_interval=None,
            gender_multi_select_values=None,
            therapy_multi_select=None,
            therapy_regimen_multi_select=None,
            treatment_naive_multi_select=None,
            prior_treatment_multi_select=None,
            primary_tumor_multi_select=[],
            tug_range_slider=None,
            tug_include_nans=None,
            hgs_range_slider=None,
            hgs_include_nans=None,
        )
        self.assertEqual(
            len(result_df),
            5
        )

    def test_filter_by_primary_tumor_none_selected(self):
        result_df = filter_patients(
            patient_cofactors=self.df,
            ecog_values=None,
            age_interval=None,
            gender_multi_select_values=None,
            therapy_multi_select=None,
            therapy_regimen_multi_select=None,
            treatment_naive_multi_select=None,
            prior_treatment_multi_select=None,
            primary_tumor_multi_select=[
                "Gastric",
                "Lung"
            ],
            tug_range_slider=None,
            tug_include_nans=None,
            hgs_range_slider=None,
            hgs_include_nans=None,
        )
        self.assertEqual(
            len(result_df),
            3
        )

    def test_filter_by_tug_include_nans(self):
        result_df = filter_patients(
            patient_cofactors=self.df,
            ecog_values=None,
            age_interval=None,
            gender_multi_select_values=None,
            therapy_multi_select=None,
            therapy_regimen_multi_select=None,
            treatment_naive_multi_select=None,
            prior_treatment_multi_select=None,
            primary_tumor_multi_select=None,
            tug_range_slider=("0.0", "10.1"),
            tug_include_nans=["Include nans"],
            hgs_range_slider=None,
            hgs_include_nans=None,
        )
        self.assertEqual(
            len(result_df),
            4
        )

    def test_filter_by_tug_exclude_nans(self):
        result_df = filter_patients(
            patient_cofactors=self.df,
            ecog_values=None,
            age_interval=None,
            gender_multi_select_values=None,
            therapy_multi_select=None,
            therapy_regimen_multi_select=None,
            treatment_naive_multi_select=None,
            prior_treatment_multi_select=None,
            primary_tumor_multi_select=None,
            tug_range_slider=("0.0", "10.1"),
            tug_include_nans=[],
            hgs_range_slider=None,
            hgs_include_nans=None,
        )
        self.assertEqual(
            len(result_df),
            2
        )
    
    def test_filter_by_hgs_include_nans(self):
        result_df = filter_patients(
            patient_cofactors=self.df,
            ecog_values=None,
            age_interval=None,
            gender_multi_select_values=None,
            therapy_multi_select=None,
            therapy_regimen_multi_select=None,
            treatment_naive_multi_select=None,
            prior_treatment_multi_select=None,
            primary_tumor_multi_select=None,
            tug_range_slider=None,
            tug_include_nans=None,
            hgs_range_slider=("0.0", "33.1"),
            hgs_include_nans=["Include nans"],
        )
        self.assertEqual(
            len(result_df),
            3
        )
    
    def test_filter_by_hgs_exclude_nans(self):
        result_df = filter_patients(
            patient_cofactors=self.df,
            ecog_values=None,
            age_interval=None,
            gender_multi_select_values=None,
            therapy_multi_select=None,
            therapy_regimen_multi_select=None,
            treatment_naive_multi_select=None,
            prior_treatment_multi_select=None,
            primary_tumor_multi_select=None,
            tug_range_slider=None,
            tug_include_nans=None,
            hgs_range_slider=("0.0", "33.1"),
            hgs_include_nans=[],
        )
        self.assertEqual(
            len(result_df),
            2
        )

    # TODO
    # tug_range_slider --> Check for NAN
    # hgs_range_slider --> Check for NAN
