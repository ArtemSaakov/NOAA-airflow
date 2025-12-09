import pytest
import pandas as pd
from datetime import datetime, date
from src.transform.merge import records_to_df, merge_obs_and_hist
from src.schemas.observation import ObservationRecord
from src.schemas.historical import HistoricalDailyRecord

@pytest.fixture
def obs_model():
    return ObservationRecord(
        station_id="USW00094847",
        timestamp=datetime(2025,10,17,12,0,0),
        temperature_c=15.0,
        wind_speed_m_s=3.0
    )

@pytest.fixture
def hist_model():
    return HistoricalDailyRecord(
        station_id="USW00094847",
        record_date=date(2025,10,17),
        datatype="TMAX",
        value=20.0,
        attributes=""
    )

def test_records_to_df_list(obs_model, hist_model):
    df_obs = records_to_df([obs_model])
    df_hist = records_to_df([hist_model])
    assert isinstance(df_obs, pd.DataFrame)
    assert isinstance(df_hist, pd.DataFrame)
    assert df_obs.shape[0] == 1
    assert "station_id" in df_obs.columns
    assert df_hist.shape[0] == 1
    assert "datatype" in df_hist.columns

def test_merge_obs_and_hist_merge(obs_model, hist_model):
    df_obs = records_to_df([obs_model])
    df_hist = records_to_df([hist_model])
    merged = merge_obs_and_hist(obs_df=df_obs, hist_df=df_hist, obs_date_field="timestamp", hist_date_field="record_date")
    # Should keep same number of rows as obs
    assert merged.shape[0] == df_obs.shape[0]
    # station_id from obs should persist
    assert merged["station_id"].iloc[0] == "USW00094847"
    # datatype from hist should appear
    assert merged["datatype"].iloc[0] == "TMAX"
    # Ensure month_day field added from obs date conversion
    assert merged["month_day"].iloc[0] == "10-17"
