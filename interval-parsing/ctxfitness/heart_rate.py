import pandas as pd
from matplotlib import pyplot as plt
from typing import Callable

def load_and_process_heart_rate(path: str) -> pd.DataFrame:
    df = pd.read_excel(path)

    if not df["Start Time (UTC)"].is_monotonic_increasing:
        raise Exception("'Start Time (UTC)' column is sorted in ascending order. Previously this was assumed as an invariant!")

    return (
        df
        .drop("User First Name", axis=1)
        .drop("User Last Name", axis=1)
        .drop("User Email", axis=1)
        .drop("Team Names", axis=1)
        .drop("Group Names", axis=1)
        .drop("Calendar Date (Local)", axis=1)
        .drop("Start Time (UTC)", axis=1)
        .drop("Time Zone (Local)", axis=1)
        .drop("Calendar Date (UTC)", axis=1)
        .drop("Start Time (s)", axis=1)
        .drop("Time Zone (s)", axis=1)
        .drop("Status", axis=1)
        .drop("Source", axis=1)
    )

def load_and_clean_heart_rate(path: str) -> pd.DataFrame:
    df = load_and_process_heart_rate(path)
    df = df.drop_duplicates()
    df["Timestamp (Local)"] = df["Start Time (Local)"].apply(lambda d: pd.Timestamp(d)) 
    df["Date"] = df["Timestamp (Local)"].apply(lambda ts: ts.date())
    return df

# You need to provide a function that preprocesses the data accordingly
def plot_acceptance_criterion(df: pd.DataFrame, row_idx: int, acceptance_hours: int):
    colors = df.iloc[row_idx].apply(lambda h: "green" if (h >= acceptance_hours) else "gray")
    plt.bar(df.columns, df.iloc[0], color = colors)
    plt.axhline(y=acceptance_hours, color='r', linestyle='-')
    plt.xlabel("Date in study timespan")
    plt.ylabel("Hours worn")
    plt.title(f"Eligible days for {acceptance_hours} hour criterion")
    plt.xticks(rotation=90)
    plt.show()