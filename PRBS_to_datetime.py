import pandas as pd

prbs = pd.read_csv("PRBS.csv", sep=";", index_col=0)
prbs.index = pd.TimedeltaIndex(
    prbs["Time [s]"], "s", freq="20min"
)
prbs.to_csv("PRBS_datetime.csv")