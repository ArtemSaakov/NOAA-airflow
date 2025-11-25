import pandas as pd



def compute_baseline(df: pd.DataFrame) -> pd.DataFrame:
    # extract month-day
    df["month_day"] = df["date"].apply(lambda d: d[5:])  # “MM-DD”
    # group by month_day, compute mean/std
    stats = df.groupby("month_day").agg({
        "value": ["mean", "std", lambda x: x.quantile(0.1), lambda x: x.quantile(0.9)]
    })
    stats.columns = ["mean", "std", "q10", "q90"]
    stats = stats.reset_index()
    return stats
