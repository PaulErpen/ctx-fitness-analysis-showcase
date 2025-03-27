# %%

import pandas as pd
import string

from ctxdashboard.domain.patient_events import create_patient_events_sheets, PatientEventTypes
from openpyxl.worksheet.datavalidation import DataValidation

ALPHABET = string.ascii_uppercase

# %%
dailies = pd.read_excel("../data/dailies.xlsx")

# %%

for event_sheet_tuple in create_patient_events_sheets(dailies):
    tracker_id = event_sheet_tuple.tracker_id
    df_track = event_sheet_tuple.sheet
    with pd.ExcelWriter(f"./patient_timesheets/timesheet_p{tracker_id}.xlsx") as writer:
        sheet_name = f'p{tracker_id}'
        df_track.to_excel(writer, index=False, sheet_name=sheet_name)
        worksheet = writer.sheets[sheet_name]
        validation = pd.DataFrame(
            {'RECIST_TYPES': []})
        recist_idx = df_track.columns.get_loc(str(PatientEventTypes.RECIST.value))
        column_letter = ALPHABET[recist_idx]

        dv_recist = DataValidation(type='list', formula1="CR, PR, PD, SD, MR")
        dv_recist.add(f'{column_letter}1:{column_letter}{df_track.shape[0]+1}')
        worksheet.add_data_validation(dv_recist)

# %%
