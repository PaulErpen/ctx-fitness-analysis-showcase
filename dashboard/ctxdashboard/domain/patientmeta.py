from ctxdashboard.util.listable_string_enum import ListableStringEnum

class PatientMetaColumn(ListableStringEnum):
    ID = 'ID',
    TRACKER_ID = 'Tracker',
    DATE_BL = 'Date_BL',
    ECOG = 'ECOG',
    DATE_OF_BIRTH = 'Date of Birth',
    AGE = 'Age',
    GENDER = 'Gender',
    THERAPY = 'Therapy',
    THERAPY_REGIMEN = 'Therapy Regimen',
    TREATMENT_NAIVE = 'Treatment Naive',
    PRIOR_TREATMENT = 'Prior Treatment',
    PRIMARY_TUMOR = 'Primary Tumor',
    TUG = 'Fitness: TUG',
    HGS = 'Fitness: HGS',
    MEMO = 'Fitness::Memo'