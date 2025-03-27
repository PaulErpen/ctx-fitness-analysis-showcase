# %%
from typing import Any, List
from ctxdashboard.domain.patientmeta import PatientMetaColumn
import pandas as pd
import numpy as np
import math


def pick_random_from_options(options: List[Any], n: int) -> List[Any]:
    selected_options = []
    for i in range(n):
        selected_options.append(
            options[math.floor(np.random.uniform(0, 1) * len(options))]
        )
    return selected_options


df_baseline = pd.read_csv("./dashboard/scripts/patient_meta_baseline.csv")

n_pats = df_baseline.shape[0]

df_baseline[f"{PatientMetaColumn.ECOG}"] = [
    round(np.random.uniform(0, 5)) for i in range(n_pats)
]
df_baseline[f"{PatientMetaColumn.AGE.value}"] = [
    round(np.random.normal(59, 10)) for i in range(n_pats)
]
df_baseline[f"{PatientMetaColumn.GENDER.value}"] = [
    f"M" if np.random.uniform(0, 1) > 0.5 else f"F" for i in range(n_pats)
]
df_baseline[f"{PatientMetaColumn.THERAPY.value}"] = pick_random_from_options(
    [
        np.nan,
        "FOLFOX 6x nach RAPIDO-Like Schema",
        "Epirubicin, Ifosfamid",
        "FOLFOX + Nivo",
        "FOLFIRINOX",
    ],
    n_pats,
)
df_baseline[f"{PatientMetaColumn.THERAPY_REGIMEN.value}"] = pick_random_from_options(
    [
        "Chemotherapy",
        "Chemoimmunotherapy",
        "Chemotherapy",
        "Chemoimmunotherapy",
        "Chemo + Targeted",
    ],
    n_pats,
)
df_baseline[f"{PatientMetaColumn.TREATMENT_NAIVE.value}"] = df_baseline[
    f"{PatientMetaColumn.THERAPY_REGIMEN.value}"
].apply(
    lambda prev_therapy: (
        "Yes" if prev_therapy in ["Chemotherapy", "Chemoimmunotherapy"] else "No"
    )
)
df_baseline[f"{PatientMetaColumn.PRIMARY_TUMOR.value}"] = pick_random_from_options(
    [
        "Gastric",
        "Lung",
        "Lung",
        "Sarcoma",
        "Sarcoma",
    ],
    n_pats,
)
df_baseline[f"{PatientMetaColumn.PRIOR_TREATMENT.value}"] = pick_random_from_options(
    [
        "Chemo + Targeted",
        "Chemotherapy",
        "none",
        "none",
        "Immunotherapy",
    ],
    n_pats,
)
df_baseline[f"{PatientMetaColumn.TUG.value}"] = np.random.normal(8, 2, n_pats)
df_baseline[f"{PatientMetaColumn.HGS.value}"] = np.random.normal(35, 6, n_pats)

print(df_baseline)

df_baseline.to_excel("./dashboard/data/patient-meta.xlsx")

# %%
