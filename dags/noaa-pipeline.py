import requests as req
from requests.exceptions import HTTPError
import pandas as pd
import time
from http import HTTPStatus


ENDPOINT = "https://api.weather.gov/stations/"
DTW = "KDTW"
ERRORS = [
    HTTPStatus.TOO_MANY_REQUESTS,
    HTTPStatus.INTERNAL_SERVER_ERROR,
    HTTPStatus.BAD_GATEWAY,
    HTTPStatus.SERVICE_UNAVAILABLE,
    HTTPStatus.GATEWAY_TIMEOUT,
]
RETRIES = 3


def fetch_observations(station_id: str = DTW) -> dict:
    url = f"{ENDPOINT}{station_id}/observations/latest"

    for n in range(RETRIES):
        try:
            resp = req.get(url, headers={"Accept": "application/ld+json"})
            resp.raise_for_status()
            return resp.json()

        except HTTPError as exc:
            code = exc.response.status_code
            # rudimentary exponential backoff
            if code in ERRORS:
                # aka 2-4-8 second delays
                time.sleep(2 ** (n + 1))
                continue

            raise

def parse_observation_json(obs_data: dict) -> dict:
    props = obs_data.get("properties", {})
    # todo: pull relevant fields
    return {
        # "timestamp": props.get("timestamp"),
        # "temperature": props.get("temperature", {}).get("value"),
        # "windSpeed": props.get("windSpeed", {}).get("value"),
        # "windDirection": props.get("windDirection", {}).get("value"),
        # "textDescription": props.get("textDescription"),
        props
    }

def fetch_historical(station_id: str = DTW) -> pd.DataFrame:
