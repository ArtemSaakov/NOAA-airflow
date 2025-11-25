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

NOAA_ENDPOINT = "https://www.ncei.noaa.gov/access/services/data/v1"
NOAA_STATION = "USW00094847"
ERRORS = [
    HTTPStatus.TOO_MANY_REQUESTS,
    HTTPStatus.INTERNAL_SERVER_ERROR,
    HTTPStatus.BAD_GATEWAY,
    HTTPStatus.SERVICE_UNAVAILABLE,
    HTTPStatus.GATEWAY_TIMEOUT,
]
RETRIES = 3


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
    
    # Fall back to cred.json
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


_TOKEN_CACHE = None


def fetch_historical(start_date: str, end_date: str, token: str = None, station_id: str = NOAA_STATION) -> list:
    """Fetch historical daily summaries from NOAA GHCND API.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        token: NOAA API token (defaults to loaded token if not provided)
        station_id: NOAA station ID (GHCND code)
    
    Returns:
        List of daily summary records from NOAA API
        
    Raises:
        HTTPError: If API returns non-retryable error after retries
    """
    if token is None:
        token = get_token()
    
    params = {
        "dataset": "daily-summaries",
        "stations": station_id,
        "startDate": start_date,
        "endDate": end_date,
        "format": "json",
        "includeAttributes": "true",
    }

    headers = {"token": token,
               "Accept": "application/json"}

    for n in range(RETRIES):
        try:
            resp = req.get(NOAA_ENDPOINT, headers=headers, params=params)
            resp.raise_for_status()
            data = resp.json().get("results", [])
            logger.info(f"Successfully fetched {len(data)} records from NOAA for {station_id} "
                       f"({start_date} to {end_date})")
            return data

        except HTTPError as exc:
            code = exc.response.status_code
            # rudimentary exponential backoff
            if code in ERRORS:
                wait_time = 2 ** (n + 1)
                logger.warning(f"NOAA API returned {code}. Retrying in {wait_time}s "
                             f"(attempt {n+1}/{RETRIES})")
                time.sleep(wait_time)
                continue
            
            # Non-retryable error
            logger.error(f"NOAA API returned non-retryable error {code}: {exc.response.text}")
            raise
        
        except Exception as exc:
            logger.error(f"Unexpected error fetching from NOAA (attempt {n+1}/{RETRIES}): {exc}")
            if n == RETRIES - 1:
                raise
            time.sleep(2 ** (n + 1))
            continue
    
    # Should not reach here, but provide fallback
    raise RuntimeError(f"Failed to fetch NOAA data after {RETRIES} retries")

def process_historical(data: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(data)
    # keep only date and tavg columns
    df = df[["DATE", "TAVG"]]
    # rename columns
    df = df.rename(columns={"DATE": "date", "TAVG": "temp_avg"})
    # convert TAVG from tenths of degrees C to degrees C
    df["temp_avg"] = df["temp_avg"].apply(lambda x: x / 10 if pd.notnull(x) else x)
    return df