import pandas as pd


def load_and_process_daily_steps(path: str) -> pd.DataFrame:
    df = pd.read_excel(path)
    return (
        df
        .drop("User First Name", axis=1)
        .drop("User Last Name", axis=1)
        .drop("User Email", axis=1)
        .drop("Team Names", axis=1)
        .drop("Group Names", axis=1)
    )
