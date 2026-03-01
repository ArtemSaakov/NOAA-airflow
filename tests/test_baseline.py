import pytest
import pandas as pd
from datetime import date
from src.transform.baseline import compute_baseline_stats, enrich_with_baseline


@pytest.fixture
def sample_hist_records():
    """Sample historical records for baseline computation.

    Contains 3 years of TMAX data for two dates (10-17 and 10-18).
    """
    return [
        {
            "station_id": "USW00094847",
            "record_date": date(2025, 10, 17),
            "datatype": "TMAX",
            "value": 10.0,
            "attributes": "",
        },
        {
            "station_id": "USW00094847",
            "record_date": date(2024, 10, 17),
            "datatype": "TMAX",
            "value": 12.0,
            "attributes": "",
        },
        {
            "station_id": "USW00094847",
            "record_date": date(2023, 10, 17),
            "datatype": "TMAX",
            "value": 8.0,
            "attributes": "",
        },
        {
            "station_id": "USW00094847",
            "record_date": date(2025, 10, 18),
            "datatype": "TMAX",
            "value": 15.0,
            "attributes": "",
        },
        {
            "station_id": "USW00094847",
            "record_date": date(2024, 10, 18),
            "datatype": "TMAX",
            "value": 16.0,
            "attributes": "",
        },
        {
            "station_id": "USW00094847",
            "record_date": date(2023, 10, 18),
            "datatype": "TMAX",
            "value": 14.0,
            "attributes": "",
        },
    ]


@pytest.fixture
def sample_obs_df():
    """Sample observation DataFrame with two dates."""
    data = [
        {"date": pd.to_datetime("2025-10-17"), "value": 11.0},
        {"date": pd.to_datetime("2025-10-18"), "value": 15.5},
    ]
    return pd.DataFrame(data)


# Tests for compute_baseline_stats
def test_compute_baseline_stats_structure(sample_hist_records):
    # Verify output structure has correct columns
    agg = compute_baseline_stats(
        historical_records=sample_hist_records,
        value_field="value",
        group_by="month_day",
    )
    assert isinstance(agg, pd.DataFrame)
    expected_cols = {"month_day", "mean", "std", "q10", "q90"}
    assert set(agg.columns) == expected_cols


def test_compute_baseline_stats_values(sample_hist_records):
    # Verify computed statistics match expected values
    agg = compute_baseline_stats(
        historical_records=sample_hist_records,
        value_field="value",
        group_by="month_day",
    )
    # For month_day "10-17" we have values [10,12,8] → mean = 10.0
    # Pandas quantile(0.10) for [8,10,12] gives 8.4 (linear interpolation)
    row = agg.loc[agg["month_day"] == "10-17"].iloc[0]
    assert pytest.approx(row["mean"], rel=1e-3) == 10.0
    assert pytest.approx(row["std"], rel=1e-3) == 2.0  # std([8,10,12]) ≈ 2.0
    # Check quantiles are within expected range (sorted: 8, 10, 12)
    assert 8.0 <= row["q10"] <= 8.5  # Linear interpolation gives ~8.4
    assert 11.5 <= row["q90"] <= 12.0  # Linear interpolation gives ~11.6


# Tests for enrich_with_baseline
def test_enrich_with_baseline_anomaly_added(sample_obs_df, sample_hist_records):
    # Verify anomaly_z column is added with sensible values
    baseline = compute_baseline_stats(
        historical_records=sample_hist_records,
        value_field="value",
        group_by="month_day",
    )
    # assign month_day to obs
    obs_df = sample_obs_df.copy()
    obs_df["month_day"] = obs_df["date"].dt.strftime("%m-%d")
    enriched = enrich_with_baseline(
        df_obs=obs_df, baseline_df=baseline, date_field="date", join_field="month_day"
    )
    assert "anomaly_z" in enriched.columns
    # Check anomaly_z values make sense: for date "10-17", obs=11 vs mean=10 → anomaly_z ~ (11-10)/std
    val = enriched.loc[
        enriched["date"] == pd.to_datetime("2025-10-17"), "anomaly_z"
    ].iloc[0]
    assert isinstance(val, float)
    # For date "10-18", obs=15.5 vs mean ~15.0 → small anomaly
    small_val = enriched.loc[
        enriched["date"] == pd.to_datetime("2025-10-18"), "anomaly_z"
    ].iloc[0]
    assert abs(small_val) < 2  # should not be huge anomaly
