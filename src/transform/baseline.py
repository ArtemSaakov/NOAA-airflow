import pandas as pd
from typing import List
from src.schemas.historical import HistoricalDailyRecord

def compute_baseline_stats(
    historical_records: List[HistoricalDailyRecord],
    value_field: str = "value",
    group_by: str = "month_day"
) -> pd.DataFrame:
    """
    Given a list of HistoricalDailyRecord, compute baseline statistics by calendar day.
    Returns DataFrame with columns: month_day, mean, std, q10, q90.
    """
    # Convert list of records to DataFrame
    df = pd.DataFrame([rec.model_dump() for rec in historical_records])
    # Add month_day field
    df["month_day"] = df["record_date"].dt.strftime("%m-%d")
    # Group by month_day
    agg = df.groupby("month_day")[value_field].agg(
        mean="mean",
        std="std",
        q10=lambda x: x.quantile(0.10),
        q90=lambda x: x.quantile(0.90),
    ).reset_index()
    return agg

def enrich_with_baseline(
    df_obs: pd.DataFrame,
    baseline_df: pd.DataFrame,
    date_field: str = "date",
    join_field: str = "month_day"
) -> pd.DataFrame:
    """
    Given an observations DataFrame and baseline stats DataFrame,
    add baseline_mean, baseline_std, baseline_q10, baseline_q90 to the obs DataFrame.
    Assumes obs DataFrame has a date field.
    """
    obs = df_obs.copy()
    obs[join_field] = obs[date_field].dt.strftime("%m-%d")
    merged = obs.merge(
        baseline_df,
        how="left",
        on=join_field,
        suffixes=("", "_baseline")
    )
    # Optionally compute anomaly
    merged["anomaly_z"] = (
        (merged["value"] - merged["mean"]) / merged["std"]
    )
    return merged
