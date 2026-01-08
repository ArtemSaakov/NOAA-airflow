import pandas as pd
from src.schemas.observation import ObservationRecord
from src.schemas.historical import HistoricalDailyRecord
from typing import List, Union


def records_to_df(
    records: List[Union[ObservationRecord, HistoricalDailyRecord]]
) -> pd.DataFrame:
    """
    Convert list of Pydantic models (ObservationRecord or HistoricalDailyRecord)
    to a pandas DataFrame.
    """
    return pd.DataFrame([rec.model_dump() for rec in records])


def merge_obs_and_hist(
    obs_df: pd.DataFrame,
    hist_df: pd.DataFrame,
    obs_date_field: str = "timestamp",
    hist_date_field: str = "record_date"
) -> pd.DataFrame:
    """
    Merge observations DataFrame and historical daily summary DataFrame
    on station_id and day-of-year (month-day). Returns merged DataFrame.
    """
    obs = obs_df.copy()
    # Convert obs timestamp to date
    obs["date"] = obs[obs_date_field].dt.date
    obs["month_day"] = obs["date"].apply(lambda d: d.strftime("%m-%d"))

    hist = hist_df.copy()
    hist["month_day"] = hist[hist_date_field].apply(
        lambda d: d.strftime("%m-%d"))

    before_rows = len(obs)
    merged = obs.merge(
        hist,
        how="left",
        on=["station_id", "month_day"],
        suffixes=("_obs", "_hist")
    )
    after_rows = len(merged)
    # Log merge stats (include before/after counts)
    missing_matches = merged["mean"].isna().sum(
    ) if "mean" in merged.columns else None
    if missing_matches is not None:
        logger = __import__("logging").getLogger(__name__)
        logger.info(
            f"Merged obs ({before_rows} -> {after_rows} rows) with hist; {missing_matches} rows had no historical match")

    return merged
