# Time Series Dataset (Sample Dataset)
import pandas as df
import pandas as pd
import numpy as np


df = pd.DataFrame(
    data={"price": np.arange(0, 100), "cost": np.arange(0, 100)}, columns=["a", "b"]
)
df["datetime"] = pd.date_range(start="2020-03-15 7:00:00", periods=100, freq="15min")
df["date"] = pd.date_range(
    start="2020-03-15 7:00:00", periods=100, freq="15min"
).strftime("%Y-%m-%d")
df.to_csv("../datasets/sample_dataframe/sample_dataframe.csv", index=False)
# Export CSV


# Time Series Dataset (Sample Dataset)
df = pd.DataFrame(
    data={"price": np.arange(0, 100), "cost": np.arange(0, 100)}, columns=["a", "b"]
)
df["datetime"] = pd.date_range(start="2020-03-15 7:00:00", periods=100, freq="15min")
df["date"] = pd.date_range(
    start="2020-03-15 7:00:00", periods=100, freq="15min"
).strftime("%Y-%m-%d")
df.to_csv("../datasets/sample_dataframe/sample_dataframe.csv", index=False)
# Export CSV
