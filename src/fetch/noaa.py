"""NOAA GHCND API fetcher with retry logic and credential management.

This module fetches historical daily weather summaries from NOAA's Global Historical
Climatology Network - Daily (GHCND) dataset.

**Data Transformations:**
- TAVG (average temperature) is returned by NOAA in tenths of °C (e.g., 225 = 22.5°C)
- process_historical() divides by 10 to convert to standard °C units
- This transformation happens BEFORE schema validation, so schemas expect data in °C
- Supported datatypes: TMAX, TMIN, TAVG, PRCP, SNOW, SNWD (defined in HistoricalDailyRecord schema)

**Credentials:**
- Token loaded from NOAA_TOKEN environment variable (preferred)
- Falls back to cred.json file in project root (loaded lazily, on first API call)
- See _load_token() and get_token() for details

**Retry Strategy:**
- Exponential backoff (2-4-8 seconds) for retriable HTTP errors (429, 500, 502, 503, 504)
- Non-retriable errors (401, 403, 404, etc.) raise immediately
- See ERRORS list and fetch_historical() for details
"""

import requests as req
from requests.exceptions import HTTPError
import time
from http import HTTPStatus
import json
from pathlib import Path
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

_TOKEN_CACHE = ""

NOAA_ENDPOINT = "https://www.ncei.noaa.gov/access/services/data/v1"
# Default station ID: Detroit Metropolitan Airport weather station
# Override via fetch_historical(station_id=...) for other locations
NOAA_STATION_DEFAULT = "USW00094847"

# HTTP status codes that warrant retry with exponential backoff
RETRIABLE_ERRORS = [
    HTTPStatus.TOO_MANY_REQUESTS,  # 429: Rate limit; aggressive backoff
    HTTPStatus.INTERNAL_SERVER_ERROR,  # 500: Server error; try again
    HTTPStatus.BAD_GATEWAY,  # 502: Temporary routing; try again
    HTTPStatus.SERVICE_UNAVAILABLE,  # 503: Service overloaded; try again
    HTTPStatus.GATEWAY_TIMEOUT,  # 504: Timeout; try again
]
# HTTP status codes that should fail fast (no retry)
FAIL_FAST_ERRORS = [
    HTTPStatus.UNAUTHORIZED,  # 401: Invalid token; never retries
    HTTPStatus.FORBIDDEN,  # 403: Access denied; never retries
    HTTPStatus.NOT_FOUND,  # 404: Endpoint doesn't exist; never retries
    HTTPStatus.BAD_REQUEST,  # 400: Malformed request; never retries
]
RETRIES = 3


def _calculate_backoff_delay(attempt: int, error_code: HTTPStatus) -> int:
    """Calculate backoff delay in seconds based on error type and attempt number.

    Args:
        attempt: 0-indexed attempt number (0, 1, 2, ...)
        error_code: HTTP status code from the failed request

    Returns:
        Delay in seconds before retry

    Strategy:
        - 429 (rate limit): aggressive exponential backoff (3-6-12 seconds)
        - Other retriable: gentle exponential backoff (2-4-8 seconds)
    """
    if error_code == HTTPStatus.TOO_MANY_REQUESTS:
        # Rate limit: back off aggressively
        return 3 * (2**attempt)  # 3, 6, 12 seconds
    else:
        # Other server errors: gentle backoff
        return 2 * (2**attempt)  # 2, 4, 8 seconds


def _load_token() -> str:
    """Load NOAA token from environment variable or cred.json file.

    Tries environment variable NOAA_TOKEN first, then falls back to cred.json.
    Raises ValueError if token cannot be loaded.
    """
    # Try environment variable first
    token = os.environ.get("NOAA_TOKEN")
    if token:
        logger.info("Loaded NOAA token from environment variable")
        return token

    # Fall back to cred.json method:
    # Utilizes file named cred.json in project root
    # and expects JSON keyword 'token'
    try:
        cred_path = Path(__file__).parent.parent.parent / "cred.json"
        token = json.loads(cred_path.read_text())["token"]
        logger.info(f"Loaded NOAA token from {cred_path}")
        return token
    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        raise ValueError(
            f"Could not load NOAA token. Set NOAA_TOKEN environment variable "
            f"or ensure {cred_path} exists with valid JSON containing 'token' key"
        ) from e


def get_token() -> str:
    """Get NOAA token (lazy loaded on first call)."""
    global _TOKEN_CACHE
    if "_TOKEN_CACHE" not in globals():
        _TOKEN_CACHE = _load_token()
    return _TOKEN_CACHE


def fetch_historical(
    start_date: str,
    end_date: str,
    token: str | None = None,
    station_id: str | None = None,
) -> list:
    """Fetch historical daily summaries from NOAA GHCND API.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        token: NOAA API token (defaults to loaded token if not provided)
        station_id: NOAA station ID (GHCND code, e.g., 'USW00094847' for Detroit).
                   Defaults to NOAA_STATION_DEFAULT if not provided.

    Returns:
        List of daily summary records from NOAA API

    Raises:
        HTTPError: If API returns non-retriable error after retries
    """
    if not token:
        token = get_token()
    if not station_id:
        station_id = NOAA_STATION_DEFAULT

    params = {
        "dataset": "daily-summaries",
        "stations": station_id,
        "startDate": start_date,
        "endDate": end_date,
        "format": "json",
        "includeAttributes": "true",
    }

    headers = {"token": token, "Accept": "application/json"}

    for n in range(RETRIES):
        try:
            resp = req.get(NOAA_ENDPOINT, headers=headers, params=params)
            resp.raise_for_status()
            data = resp.json().get("results", [])
            logger.info(
                f"Successfully fetched {len(data)} records from NOAA for {station_id} "
                f"({start_date} to {end_date})"
            )
            return data

        except HTTPError as exc:
            code = exc.response.status_code
            # Check if error is retriable
            if code in RETRIABLE_ERRORS:
                delay = _calculate_backoff_delay(n, HTTPStatus(code))
                logger.warning(
                    f"NOAA API returned {code}. Retrying in {delay}s "
                    f"(attempt {n+1}/{RETRIES})"
                )
                time.sleep(delay)
                continue

            # Fail-fast errors and other non-retriable errors
            logger.error(
                f"NOAA API returned non-retriable error {code}: {exc.response.text}"
            )
            raise

        except Exception as exc:
            logger.error(
                f"Unexpected error fetching from NOAA (attempt {n+1}/{RETRIES}): {exc}"
            )
            if n == RETRIES - 1:
                raise
            time.sleep(2 ** (n + 1))
            continue

    # Should not reach here, but provide fallback
    raise RuntimeError(f"Failed to fetch NOAA data after {RETRIES} retries")


def process_historical(data: list[dict]) -> pd.DataFrame:
    """Process raw NOAA GHCND historical records into standard format.

    **Important Data Transformations:**
    - Extracts only TAVG (daily average temperature) from raw records
    - Converts TAVG from tenths of °C to °C (e.g., 225 → 22.5)
    - NOAA API returns temperature data in tenths of degrees; this is a known quirk
    - See module docstring for details on data units and schemas

    Args:
        data: List of dicts from NOAA API response, each containing DATE, TAVG, etc.

    Returns:
        DataFrame with columns: date (YYYY-MM-DD), temp_avg (°C)
        Rows with missing TAVG values are preserved with NaN in temp_avg

    Raises:
        KeyError: If expected columns (DATE, TAVG) are missing from input
    """
    df = pd.DataFrame(data)
    # keep only date and tavg columns
    df = df[["DATE", "TAVG"]]
    # rename columns
    df = df.rename(columns={"DATE": "date", "TAVG": "temp_avg"})
    # IMPORTANT: convert TAVG from tenths of degrees C to degrees C
    # NOAA GHCND API returns all temperature values as 10x their actual value
    # Example: 225 in raw data = 22.5°C after conversion
    df["temp_avg"] = df["temp_avg"].apply(lambda x: x / 10 if pd.notnull(x) else x)
    return df
