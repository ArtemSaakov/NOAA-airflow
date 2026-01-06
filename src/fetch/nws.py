"""NWS API fetcher with retry logic for real-time weather observations.

This module fetches latest observations from NOAA's National Weather Service (NWS) API.

**Data Units:**
- Temperature: degrees Celsius (°C)
- Wind speed: meters per second (m/s)
- Wind direction: degrees (0-360)
- All values from NWS API are already in SI units (no conversion needed)

**Credentials:**
- NWS API requires no authentication (public endpoint)

**Retry Strategy:**
- Exponential backoff (2-4-8 seconds) for retriable HTTP errors (429, 500, 502, 503, 504)
- Non-retriable errors (401, 403, 404, etc.) raise immediately
"""

import requests as req
from requests.exceptions import HTTPError
import time
from http import HTTPStatus
import logging

logger = logging.getLogger(__name__)

NWS_ENDPOINT = "https://api.weather.gov/stations/"
# Default station ID: Detroit Metro Airport weather station
# Override via fetch_observations(station_id=...) for other locations
NWS_STATION_DEFAULT = "KDTW"

# HTTP status codes that warrant retry with exponential backoff
RETRIABLE_ERRORS = [
    HTTPStatus.TOO_MANY_REQUESTS,          # 429: Rate limit; aggressive backoff
    HTTPStatus.INTERNAL_SERVER_ERROR,      # 500: Server error; try again
    HTTPStatus.BAD_GATEWAY,                # 502: Temporary routing; try again
    HTTPStatus.SERVICE_UNAVAILABLE,        # 503: Service overloaded; try again
    HTTPStatus.GATEWAY_TIMEOUT,            # 504: Timeout; try again
]
# HTTP status codes that should fail fast (no retry)
FAIL_FAST_ERRORS = [
    HTTPStatus.UNAUTHORIZED,               # 401: Invalid token; never retries
    HTTPStatus.FORBIDDEN,                  # 403: Access denied; never retries
    HTTPStatus.NOT_FOUND,                  # 404: Endpoint doesn't exist; never retries
    HTTPStatus.BAD_REQUEST,                # 400: Malformed request; never retries
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
        return 3 * (2 ** attempt)  # 3, 6, 12 seconds
    else:
        # Other server errors: gentle backoff
        return 2 * (2 ** attempt)  # 2, 4, 8 seconds


def fetch_observations(station_id: str | None = None) -> dict:
    """Fetch latest observations from NWS API.

    Args:
        station_id: NWS station identifier (e.g., 'KDTW' for Detroit Metro Airport).
                   Defaults to NWS_STATION_DEFAULT if not provided.

    Returns:
        Full JSON response from NWS /observations/latest endpoint with properties including
        temperature, wind_speed, wind_direction, and text_description (all SI units).

    Raises:
        HTTPError: If API returns non-retriable error after retries
    """
    if station_id is None:
        station_id = NWS_STATION_DEFAULT

    url = f"{NWS_ENDPOINT}{station_id}/observations/latest"

    for n in range(RETRIES):
        try:
            resp = req.get(url, headers={"Accept": "application/ld+json"})
            resp.raise_for_status()
            data = resp.json()
            logger.info(
                f"Successfully fetched latest observation for {station_id}")
            return data

        except HTTPError as exc:
            code = exc.response.status_code
            # Check if error is retriable
            if code in RETRIABLE_ERRORS:
                delay = _calculate_backoff_delay(n, HTTPStatus(code))
                logger.warning(f"NWS API returned {code}. Retrying in {delay}s "
                               f"(attempt {n+1}/{RETRIES})")
                time.sleep(delay)
                continue

            # Error loggerogger
            logger.error(
                f"NWS API returned non-retriable error {code}: {exc.response.text}")
            raise

        except Exception as exc:
            logger.error(
                f"Unexpected error fetching from NWS (attempt {n+1}/{RETRIES}): {exc}")
            if n == RETRIES - 1:
                raise
            time.sleep(2 ** (n + 1))
            continue

    # Should not reach here, but provide fallback
    raise RuntimeError(f"Failed to fetch NWS data after {RETRIES} retries")


def parse_observation_json(obs_data: dict) -> dict:
    """Extract relevant fields from NWS observation JSON response.

    Args:
        obs_data: Full JSON response from NWS /observations/latest endpoint

    Returns:
        Dictionary with extracted observation fields

    Note:
        NWS returns temperature and wind speed in SI units but as nested objects
        with value and unitCode fields. This extracts just the values.
    """
    props = obs_data.get("properties", {})

    # Extract nested values from NWS JSON structure
    temp_obj = props.get("temperature", {})
    wind_obj = props.get("windSpeed", {})

    temperature_c = temp_obj.get(
        "value") if isinstance(temp_obj, dict) else None
    wind_speed_m_s = wind_obj.get(
        "value") if isinstance(wind_obj, dict) else None

    return {
        "timestamp": props.get("timestamp"),
        "temperature_c": temperature_c,
        "wind_speed_m_s": wind_speed_m_s,
        "wind_direction": props.get("windDirection", {}).get("value"),
        "text_description": props.get("textDescription"),
    }
