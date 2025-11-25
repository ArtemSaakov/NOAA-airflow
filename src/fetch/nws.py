import requests as req
from requests.exceptions import HTTPError
import time
from http import HTTPStatus
import logging

logger = logging.getLogger(__name__)

NWS_ENDPOINT = "https://api.weather.gov/stations/"
ERRORS = [
    HTTPStatus.TOO_MANY_REQUESTS,
    HTTPStatus.INTERNAL_SERVER_ERROR,
    HTTPStatus.BAD_GATEWAY,
    HTTPStatus.SERVICE_UNAVAILABLE,
    HTTPStatus.GATEWAY_TIMEOUT,
]
NWS_STATION = "KDTW"  # Detroit Metro Airport
RETRIES = 3


def fetch_observations(station_id: str = NWS_STATION) -> dict:
    """Fetch latest observations from NWS API.

    Args:
        station_id: NWS station identifier (e.g., KDTW)

    Returns:
        Full JSON response from NWS /observations/latest endpoint

    Raises:
        HTTPError: If API returns non-retryable error after retries
    """
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
            # rudimentary exponential backoff
            if code in ERRORS:
                wait_time = 2 ** (n + 1)
                logger.warning(f"NWS API returned {code}. Retrying in {wait_time}s "
                               f"(attempt {n+1}/{RETRIES})")
                time.sleep(wait_time)
                continue

            # Non-retryable error
            logger.error(
                f"NWS API returned non-retryable error {code}: {exc.response.text}")
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
