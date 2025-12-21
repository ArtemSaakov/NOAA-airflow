"""Airflow DAG to run NOAA/NWS fetch-merge-compute pipeline.

This file is guarded so it won't raise import errors when Airflow is not
installed in the development environment (e.g., when running unit tests).

The DAG implementation uses small Python callables that call into the
src.fetch and src.transform modules and writes intermediate CSVs to
data/ so results can be inspected outside Airflow. Internal modules and heavy
imports are performed lazily inside task callables to keep things lightweight
and streamlined, particularly for unit testing.
"""
from __future__ import annotations
import os
from datetime import datetime, timedelta
from pathlib import Path
import logging
import pandas as pd

try:
    # Attempt to import Airflow module for dev/local envs
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except Exception:    # pragma: no cover - environment specific
    AIRFLOW_AVAILABLE = False


LOG = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def _iso_today() -> str:
    return datetime.now(datetime.UTC).isoformat()


def fetch_noaa_task(**context):
    """Fetch historical NOAA data and and  to CSV."""
    from src.fetch import noaa

    # Default: last 30 days of data (can be overridden in a production DAG)
    end_date = datetime.now(datetime.UTC).isoformat()
    start_date = end_date - timedelta(days=30)

    LOG.info("Fetching NOAA historical data %s -> %s", start_date, end_date)
    data = noaa.fetch_historical(
        start_date=start_date.isoformat(), end_date=end_date.isoformat())
    df = noaa.process_historical(data)
    # Normalize column names expected by downstream transforms
    #
    # process_historical returns `date` and `temp_avg` (°C)
    # Convert to schema-like dicts for easier downstream use
    out_path = DATA_DIR / f"noaa_hist_{_iso_today()}.csv"
    df = df.rename(columns={"date": "record_date", "temp_avg": "value"})
    # Add required static columns for HistoricalDailyRecord compat
    df["station_id"] = os.environ.get(
        "NOAA_STATION", noaa.NOAA_STATION_DEFAULT)
    df["datatype"] = "TAVG"
    df["attributes"] = ""
    # Ensure record_date is ISO string
    df["record_date"] = pd.to_datetime(df["record_date"]).dt.date
    df.to_csv(out_path, index=False)
    LOG.info("Wrote NOAA historical CSV: %s", out_path)


def fetch_nws_task(**context):
    """Fetch latest NWS observation and persist to CSV."""
    from src.fetch import nws

    LOG.info("Fetching NWS latest observation for station %s",
             os.environ.get("NWS_STATION", nws.NWS_STATION_DEFAULT))
    obs_json = nws.fetch_observations()
    obs = nws.parse_observation_json(obs_json)
    df = pd.DataFrame([obs])
    # Normalize fields: timestamp -> datetime
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    out_path = DATA_DIR / f"nws_obs_{_iso_today()}.csv"
    df.to_csv(out_path, index=False)
    LOG.info("Wrote NWS observation CSV: %s", out_path)


def merge_and_baseline_task(**context):
    """Merge the latest observation with the historical baseline and write merged CSV."""
    from src.transform import merge as merge_mod
    from src.transform import baseline as baseline_mod

    # Check for latest files (sorting alphabetically for simpliicty)
    noaa_files = sorted(DATA_DIR.glob("noaa_hist_*.csv"))
    nws_files = sorted(DATA_DIR.glob("nws_obs_*.csv"))
    if not noaa_files or not nws_files:
        LOG.warning(
            "Missing input files for merge/baseline: noaa=%s nws=%s", noaa_files, nws_files)
        return

    noaa_df = pd.read_csv(noaa_files[-1])
    nws_df = pd.read_csv(nws_files[-1])

    # Prepare dataframes to match transform expectations
    # Historical: record_date -> datetime
    noaa_df["record_date"] = pd.to_datetime(noaa_df["record_date"]).dt.date
    # Observations: timestamp -> datetime
    if "timestamp" in nws_df.columns:
        nws_df["timestamp"] = pd.to_datetime(nws_df["timestamp"])
    # Ensure station_id present on observations
    if "station_id" not in nws_df.columns:
        nws_df["station_id"] = os.environ.get("NWS_STATION", "KDTW")

    # Call merge function which expects DataFrames
    merged = merge_mod.merge_obs_and_hist(
        nws_df, noaa_df, obs_date_field="timestamp", hist_date_field="record_date")

    # Compute baseline from historical records (pass list of dicts)
    hist_records = noaa_df.to_dict(orient="records")
    baseline_df = baseline_mod.compute_baseline_stats(
        hist_records, value_field="value")

    # Enrich merged with baseline (expects df_obs with date field; ensure a `date` exists)
    if "date" not in merged.columns:
        merged["date"] = merged["timestamp"].dt.date

    enriched = baseline_mod.enrich_with_baseline(merged, baseline_df, date_field="date", value_field=(
        "temperature_c" if "temperature_c" in merged.columns else "value"))

    out_path = DATA_DIR / f"merged_enriched_{_iso_today()}.csv"
    enriched.to_csv(out_path, index=False)
    LOG.info("Wrote merged+enriched CSV: %s", out_path)


if AIRFLOW_AVAILABLE:  # pragma: no cover - runtime-only
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
    }

    with DAG(
            dag_id="weather_pipeline",
            default_args=default_args,
            schedule_interval=None,
            start_date=datetime(2025, 1, 1),
            catchup=False,
            tags=["noaa", "nws", "baseline"],
    ) as dag:

        t_fetch_noaa = PythonOperator(
            task_id="fetch_noaa", python_callable=fetch_noaa_task)
        t_fetch_nws = PythonOperator(
            task_id="fetch_nws", python_callable=fetch_nws_task)
        t_merge_baseline = PythonOperator(
            task_id="merge_and_baseline", python_callable=merge_and_baseline_task)

        t_fetch_noaa >> t_fetch_nws >> t_merge_baseline

else:  # Provide a safe fallback so importing this module in tests won't error out
    dag = None
    LOG.info(
        "Airflow not available; `dags/weather_pipeline.py` loaded in lightweight mode")
