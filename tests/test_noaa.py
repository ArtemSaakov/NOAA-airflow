import pytest
import pandas as pd
from unittest.mock import patch, Mock
from http import HTTPStatus
from requests.exceptions import HTTPError
from src.fetch.noaa import (
    _calculate_backoff_delay,
    _load_token,
    get_token,
    fetch_historical,
    process_historical,
)


@pytest.fixture
def sample_noaa_response():
    """Sample NOAA API response with multiple records."""
    return [
        {
            "DATE": "2025-10-17",
            "STATION": "USW00094847",
            "TAVG": 225,  # 22.5°C in tenths
            "TMAX": 280,
            "TMIN": 170,
        },
        {
            "DATE": "2025-10-18",
            "STATION": "USW00094847",
            "TAVG": 210,  # 21.0°C in tenths
            "TMAX": 260,
            "TMIN": 160,
        },
    ]


@pytest.fixture
def sample_noaa_response_missing_tavg():
    """Sample NOAA API response with missing TAVG data."""
    return [
        {
            "DATE": "2025-10-17",
            "STATION": "USW00094847",
            "TMAX": 280,
            "TMIN": 170,
        },
    ]


@pytest.fixture
def sample_noaa_response_with_nulls():
    """Sample NOAA API response with null TAVG values."""
    return [
        {
            "DATE": "2025-10-17",
            "STATION": "USW00094847",
            "TAVG": 225,
            "TMAX": 280,
        },
        {
            "DATE": "2025-10-18",
            "STATION": "USW00094847",
            "TAVG": None,  # Missing temperature
            "TMAX": 260,
        },
    ]


# Tests for _calculate_backoff_delay
def test_calculate_backoff_delay_rate_limit():
    # Rate limit (429) gets aggressive backoff: 3-6-12 seconds
    assert _calculate_backoff_delay(0, HTTPStatus.TOO_MANY_REQUESTS) == 3
    assert _calculate_backoff_delay(1, HTTPStatus.TOO_MANY_REQUESTS) == 6
    assert _calculate_backoff_delay(2, HTTPStatus.TOO_MANY_REQUESTS) == 12


def test_calculate_backoff_delay_server_error():
    # Server errors get gentle backoff: 2-4-8 seconds
    assert _calculate_backoff_delay(0, HTTPStatus.INTERNAL_SERVER_ERROR) == 2
    assert _calculate_backoff_delay(1, HTTPStatus.INTERNAL_SERVER_ERROR) == 4
    assert _calculate_backoff_delay(2, HTTPStatus.INTERNAL_SERVER_ERROR) == 8


def test_calculate_backoff_delay_service_unavailable():
    # Service unavailable gets gentle backoff
    assert _calculate_backoff_delay(0, HTTPStatus.SERVICE_UNAVAILABLE) == 2
    assert _calculate_backoff_delay(1, HTTPStatus.SERVICE_UNAVAILABLE) == 4


# Tests for _load_token
def test_load_token_success():
    with patch.dict("os.environ", {"NOAA_TOKEN": "test_token_123"}):
        token = _load_token()
        assert token == "test_token_123"


def test_load_token_missing():
    with patch.dict("os.environ", {}, clear=True):
        with pytest.raises(ValueError, match="Could not load NOAA token"):
            _load_token()


# Tests for get_token
def test_get_token_lazy_load():
    import src.fetch.noaa as noaa_module

    # Save original cache
    original_cache = noaa_module._TOKEN_CACHE
    try:
        # Reset cache and mock the load function
        noaa_module._TOKEN_CACHE = ""
        with patch(
            "src.fetch.noaa._load_token", return_value="loaded_token"
        ) as mock_load:
            # First call should trigger load
            token = get_token()
            assert token == "loaded_token"
            assert mock_load.call_count == 1

            # Second call should use cached value
            token2 = get_token()
            assert token2 == "loaded_token"
            assert mock_load.call_count == 1  # Still only called once
    finally:
        # Restore original cache
        noaa_module._TOKEN_CACHE = original_cache


# Tests for process_historical
def test_process_historical_structure(sample_noaa_response):
    df = process_historical(sample_noaa_response)
    assert isinstance(df, pd.DataFrame)
    assert set(df.columns) == {"date", "temp_avg"}
    assert df.shape[0] == 2


def test_process_historical_value_conversion(sample_noaa_response):
    # TAVG values should be divided by 10: 225 -> 22.5, 210 -> 21.0
    df = process_historical(sample_noaa_response)
    assert df["temp_avg"].iloc[0] == 22.5
    assert df["temp_avg"].iloc[1] == 21.0


def test_process_historical_date_column(sample_noaa_response):
    df = process_historical(sample_noaa_response)
    assert df["date"].iloc[0] == "2025-10-17"
    assert df["date"].iloc[1] == "2025-10-18"


def test_process_historical_missing_tavg(sample_noaa_response_missing_tavg):
    # Should return empty DataFrame if TAVG column is missing
    df = process_historical(sample_noaa_response_missing_tavg)
    assert df.shape[0] == 0
    assert set(df.columns) == {"date", "temp_avg"}


def test_process_historical_null_values(sample_noaa_response_with_nulls):
    # Rows with null TAVG should preserve null values
    df = process_historical(sample_noaa_response_with_nulls)
    assert df.shape[0] == 2
    assert df["temp_avg"].iloc[0] == 22.5
    assert pd.isnull(df["temp_avg"].iloc[1])


def test_process_historical_missing_date():
    # Should raise ValueError if DATE column is missing
    data = [{"TAVG": 225, "STATION": "USW00094847"}]
    with pytest.raises(ValueError, match="missing DATE column"):
        process_historical(data)


# Tests for fetch_historical (with mocking)
@patch("src.fetch.noaa.req.get")
def test_fetch_historical_success(mock_get, sample_noaa_response):
    # Mock successful API response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = sample_noaa_response
    mock_get.return_value = mock_response

    result = fetch_historical(
        start_date="2025-10-17",
        end_date="2025-10-18",
        token="test_token",
        station_id="USW00094847",
    )
    assert result == sample_noaa_response
    assert mock_get.call_count == 1


@patch("src.fetch.noaa.req.get")
def test_fetch_historical_dict_response(mock_get):
    # Some NOAA responses return dict with "results" key
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"results": [{"DATE": "2025-10-17"}]}
    mock_get.return_value = mock_response

    result = fetch_historical(
        start_date="2025-10-17",
        end_date="2025-10-17",
        token="test_token",
    )
    assert result == [{"DATE": "2025-10-17"}]


@patch("src.fetch.noaa.req.get")
@patch("src.fetch.noaa.time.sleep")  # Mock sleep to speed up test
def test_fetch_historical_retry_on_429(mock_sleep, mock_get):
    # First call returns 429, second succeeds
    mock_fail = Mock()
    mock_fail.status_code = 429
    mock_fail.raise_for_status.side_effect = HTTPError(response=mock_fail)

    mock_success = Mock()
    mock_success.status_code = 200
    mock_success.json.return_value = [{"DATE": "2025-10-17"}]

    mock_get.side_effect = [mock_fail, mock_success]

    result = fetch_historical(
        start_date="2025-10-17",
        end_date="2025-10-17",
        token="test_token",
    )
    assert result == [{"DATE": "2025-10-17"}]
    assert mock_get.call_count == 2
    assert mock_sleep.call_count == 1  # Should sleep once before retry


@patch("src.fetch.noaa.req.get")
def test_fetch_historical_fail_fast_on_401(mock_get):
    # 401 (Unauthorized) should fail immediately without retry
    mock_response = Mock()
    mock_response.status_code = 401
    mock_response.text = "Unauthorized"
    mock_response.raise_for_status.side_effect = HTTPError(response=mock_response)
    mock_get.return_value = mock_response

    with pytest.raises(HTTPError):
        fetch_historical(
            start_date="2025-10-17",
            end_date="2025-10-17",
            token="invalid_token",
        )
    assert mock_get.call_count == 1  # No retries


@patch("src.fetch.noaa.req.get")
def test_fetch_historical_fail_fast_on_404(mock_get):
    # 404 (Not Found) should fail immediately without retry
    mock_response = Mock()
    mock_response.status_code = 404
    mock_response.text = "Not Found"
    mock_response.raise_for_status.side_effect = HTTPError(response=mock_response)
    mock_get.return_value = mock_response

    with pytest.raises(HTTPError):
        fetch_historical(
            start_date="2025-10-17",
            end_date="2025-10-17",
            token="test_token",
        )
    assert mock_get.call_count == 1  # No retries


@patch("src.fetch.noaa.req.get")
@patch("src.fetch.noaa.time.sleep")
def test_fetch_historical_max_retries_exceeded(mock_sleep, mock_get):
    # All retries fail with 503 (retriable error)
    mock_response = Mock()
    mock_response.status_code = 503
    mock_response.raise_for_status.side_effect = HTTPError(response=mock_response)
    mock_get.return_value = mock_response

    with pytest.raises(RuntimeError, match="Failed to fetch NOAA data after 3 retries"):
        fetch_historical(
            start_date="2025-10-17",
            end_date="2025-10-17",
            token="test_token",
        )
    assert mock_get.call_count == 3  # Should retry 3 times
    assert mock_sleep.call_count == 3  # Should sleep before each retry
