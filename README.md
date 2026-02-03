NOAA Airflow Pipeline
====================================

Whenever I want to take on a project for the sake of learning a new tool or piece of software,
I usually look to the weather for all of my data-related needs, as it tends to be public (free). In this case, I wanted to familiarize
myself with [Apache Airflow](https://airflow.apache.org/), a tool for managing data workflows. So I made a workflow that fetches
current and historical weather data for a given day and allows for easy anomaly detection by computing z-scores. If a day's temperature deviates significantly from the historical mean for that calendar day, it can be considered "unusual."

APIs from the National Oceanic and Atmospheric Administration (NOAA) are utilized to fetch real-time and historical weather data.
Various statistics are pulled, compared, and analyzed:

- max and min temperatures
- average temperature
- precipitation totals
- snowfall totals
- historical percentile bands
- anomaly detection by identifying standard deviation in temp
- data quality checks via schema validation

## Quick Start Guide

### Prerequisites

- Docker and Docker Compose installed
- NOAA API token (free, per the above)

### 1. Get a NOAA API Token

1. Visit [NOAA NCDC Token Request](https://www.ncdc.noaa.gov/cdo-web/token)
2. Enter your email address
3. Check your email for the token (should be near instant)

### 2. Clone and Configure

```bash
git clone https://github.com/ArtemSaakov/NOAA-airflow.git
cd NOAA-airflow
```

Copy the example environment file and add your credentials:

```bash
cp .env.example .env
```

Edit `.env` and set:
- `NOAA_TOKEN=your_token_here`
- `_AIRFLOW_WWW_USER_USERNAME=your_username`
- `_AIRFLOW_WWW_USER_PASSWORD=your_password`

### 3. Start Airflow

```bash
docker-compose up -d
```

Wait 1-2 minutes for services to initialize, then access the Airflow UI at `http://localhost:8080`

### 4. Run the Pipeline

1. Log in with the credentials you set in `.env`
2. Start the `weather_pipeline` DAG
3. Click "Trigger DAG" to run manually

### 5. View Results

Check the `data/` directory for output CSV files with baseline statistics and anomaly z-scores.
