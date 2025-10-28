import pytest
from datetime import date, datetime
from pydantic import ValidationError
from src.schemas.historical import HistoricalDailyRecord
from src.schemas.observation import ObservationRecord

def test_observation_valid():
    obs = ObservationRecord(
        station_id="KDTW",
        timestamp=datetime(2025,10,18,12,34,56),
        temperature_c=15.3,
        wind_speed_m_s=4.2
    )
    assert obs.station_id == "KDTW"
    assert obs.temperature_c == 15.3

def test_observation_invalid_negative_wind():
    with pytest.raises(ValidationError):
        ObservationRecord(
            station_id="KDTW",
            timestamp=datetime(2025,10,18,12,34,56),
            temperature_c=10.0,
            wind_speed_m_s=-1.0
        )

def test_historical_valid():
    hist = HistoricalDailyRecord(
        station_id="USW00094847",
        record_date=date(2025,10,17),
        datatype="TMAX",
        value=22.4,
        attributes=""
    )
    assert hist.datatype == "TMAX"
    assert isinstance(hist.value, float)

def test_historical_invalid_datatype():
    with pytest.raises(ValidationError):
        HistoricalDailyRecord(
            station_id="USW00094847",
            record_date=date(2025,10,17),
            datatype="UNKNOWN",
            value=10.0,
            attributes=None
        )

def test_historical_value_negative_when_precip():
    with pytest.raises(ValidationError):
        HistoricalDailyRecord(
            station_id="USW00094847",
            record_date=date(2025,10,17),
            datatype="PRCP",
            value=-0.1,
            attributes=""
        )
