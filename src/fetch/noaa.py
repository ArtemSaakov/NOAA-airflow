import requests as req
from requests.exceptions import HTTPError
import time
from http import HTTPStatus
import json
from pathlib import Path
import pandas as pd


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
TOKEN = json.loads(Path("../cred.json").read_text())["token"]


def fetch_historical(start_date: str, end_date: str, token: str = TOKEN, station_id: str = NOAA_STATION) -> dict:
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
            return resp.json().get("results", [])

        except HTTPError as exc:
            code = exc.response.status_code
            # rudimentary exponential backoff
            if code in ERRORS:
                # aka 2-4-8 second delays
                time.sleep(2 ** (n + 1))
                continue

            raise

def process_historical(data: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(data)
    # keep only date and tavg columns
    df = df[["DATE", "TAVG"]]
    # rename columns
    df = df.rename(columns={"DATE": "date", "TAVG": "temp_avg"})
    # convert TAVG from tenths of degrees C to degrees C
    df["temp_avg"] = df["temp_avg"].apply(lambda x: x / 10 if pd.notnull(x) else x)
    return df