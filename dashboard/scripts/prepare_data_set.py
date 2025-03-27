from ctxfitness.preprocessing_pipeline import PreprocessingPipeline
import pandas as pd

path_all_dailies: str = "../../SampleData/generated_dailies.xlsx"

normalized_dailies: pd.DataFrame = PreprocessingPipeline.run_pipeline(path_all_dailies)
normalized_dailies.to_excel("../data/dailies.xlsx")