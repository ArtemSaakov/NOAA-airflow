import pandas as pd
import logging
from typing import Sequence
from src.schemas.historical import HistoricalDailyRecord

logger = logging.getLogger(__name__)


def compute_baseline_stats(
    historical_records: Sequence[HistoricalDailyRecord | dict],
    value_field: str = "value",
    group_by: str = "month_day",
    years_back: int | None = None,
) -> pd.DataFrame:
    """
    Given a list of HistoricalDailyRecord or dicts, compute baseline
    statistics by calendar day.
    Returns DataFrame with columns: month_day, mean, std, q10, q90.

    Args:
        historical_records: List of HistoricalDailyRecord or dicts with record_date and value
        value_field: Name of field containing the values to aggregate
        group_by: Field name to group by (will be created from record_date if needed)

    Returns:
        DataFrame with columns: month_day, mean, std, q10, q90
    """
    # Convert list of records to DataFrame, handling both Pydantic models and dicts
    records_data = [
        rec.model_dump() if isinstance(rec, HistoricalDailyRecord) else rec
        for rec in historical_records
    ]
    df = pd.DataFrame(records_data)
    # Coerce record_date to datetime unconditionally with error handling
    if "record_date" in df:
        df["record_date"] = pd.to_datetime(df["record_date"], errors="coerce")
    # Restricting baseline to recent years to avoid long-term drift
    if years_back:
        # Determine the latest year present and compute cutoff
        max_year = df["record_date"].dt.year.max()
        min_year = int(max_year - years_back + 1)
        before_count = len(df)
        df = df[df["record_date"].dt.year >= min_year]
        after_count = len(df)
        logger.info(
            f"Filtering historical records for baseline: kept {after_count}/{before_count} records from last {years_back} years (>= {min_year})"
        )
    # Ensure the grouping field exists. If it's missing and can be derived from
    # `record_date` (the common case for `month_day`), create it. Otherwise
    # raise a clear error so callers know to provide the column.
    if group_by not in df:
        if group_by == "month_day" and "record_date" in df:
            df[group_by] = df["record_date"].dt.strftime("%m-%d")
        else:
            raise ValueError(
                f"group_by='{group_by}' not present in records and cannot be derived from record_date"
            )

    # Group by the requested field
    logger.info(
        f"Computing baseline stats from {len(df)} historical records across {df[group_by].nunique()} calendar groups (grouped by '{group_by}')"
    )
    agg = (
        df.groupby(group_by)[value_field]
        .agg(
            mean="mean",
            std="std",
            q10=lambda x: x.quantile(0.10),
            q90=lambda x: x.quantile(0.90),
        )
        .reset_index()
    )
    return agg


def enrich_with_baseline(
    df_obs: pd.DataFrame,
    baseline_df: pd.DataFrame,
    date_field: str = "date",
    join_field: str = "month_day",
    value_field: str = "value",
) -> pd.DataFrame:
    """
    Given an observations DataFrame and baseline stats DataFrame,
    add baseline_mean, baseline_std, baseline_q10, baseline_q90 to the obs DataFrame
    and compute anomaly z-scores.

    Args:
        df_obs: Observations DataFrame with a date field
        baseline_df: Baseline stats DataFrame with month_day, mean, std, q10, q90
        date_field: Name of date field in df_obs
        join_field: Name of field to join on (typically month_day)
        value_field: Name of value field in df_obs to compute anomaly for

    Returns:
        DataFrame with baseline columns and anomaly_z; NaN for anomaly_z when
        baseline is missing or std is zero.

    Warning:
        Rows without baseline matches will have NaN for anomaly_z.
        Rows where std=0 will have NaN (or inf if mean differs) for anomaly_z.
    """
    obs = df_obs.copy()
    obs[join_field] = obs[date_field].dt.strftime("%m-%d")
    merged = obs.merge(
        baseline_df, how="left", on=join_field, suffixes=("", "_baseline")
    )

    # Compute anomaly z-score with safe division
    # Division by zero returns inf; NaN from missing baseline is preserved
    merged["anomaly_z"] = pd.NA  # Initialize as nullable

    # Only compute where both value and mean exist and std is non-zero
    valid_mask = (
        merged[value_field].notna()
        & merged["mean"].notna()
        & merged["std"].notna()
        & (merged["std"] != 0)
    )

    merged.loc[valid_mask, "anomaly_z"] = (
        merged.loc[valid_mask, value_field] - merged.loc[valid_mask, "mean"]
    ) / merged.loc[valid_mask, "std"]

    return merged
