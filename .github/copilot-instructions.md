# NOAA-Airflow Copilot Instructions

## Project Overview
NOAA-Airflow is an Apache Airflow orchestration project for weather data ingestion, transformation, and analysis. It fetches historical NOAA climate data and real-time NWS observations, merges them, computes baseline statistics, and detects temperature anomalies.

## Architecture

### Data Pipeline Flow
1. **Fetch**: Two data sources via HTTP with exponential backoff retry logic:
   - `src/fetch/noaa.py`: Calls NOAA GHCND API for historical daily summaries (TMAX, TMIN, PRCP, etc.)
   - `src/fetch/nws.py`: Calls NWS API for real-time observations (temperature, wind speed)
2. **Schema Validation**: Pydantic v2 models enforce data integrity:
   - `src/schemas/historical.py`: `HistoricalDailyRecord` - validates datatypes (TMAX, TMIN, TAVG, PRCP, SNOW, SNWD) and non-negative values
   - `src/schemas/observation.py`: `ObservationRecord` - validates temperature/wind are non-negative
3. **Transform & Analysis**:
   - `src/transform/merge.py`: Joins observations and historical data by (station_id, month_day)
   - `src/transform/baseline.py`: Computes statistics (mean, std, q10, q90) by calendar day; enriches observations with anomaly scores (z-scores)
4. **Orchestration**: `dags/computations.py` - Airflow DAG definition

### Key Integration Points
- **External APIs**: NOAA (token-based, rate-limited) and NWS (no auth)
- **Retry Strategy**: Exponential backoff (2-4-8 seconds) for 429, 500, 502, 503, 504 errors (defined in `ERRORS` lists)
- **Credentials**: NOAA token stored in `cred.json` (loaded at import time in `noaa.py`)

## Development Workflows

### Setup
```bash
source airflow-venv/bin/activate
```
Virtual environment already provisioned with pandas, pydantic, requests, pytest, numpy, and Airflow dependencies.

### Testing
```bash
pytest tests/
pytest tests/test_schemas.py -v          # Schema validation
pytest tests/test_baseline.py -v         # Baseline statistics
pytest tests/test_merge.py -v            # Data merging
```
Tests use pytest fixtures with sample records and DataFrames. Leverage parametrize for multi-datatype/station tests.

### Running Airflow Locally
```bash
docker-compose up -d                     # Start Airflow + PostgreSQL + Redis
# Access UI at http://localhost:8080 (user: airflow / pass: airflow)
docker-compose down
```
Docker Compose configures CeleryExecutor with Redis broker and PostgreSQL backend. Volumes mount `dags/`, `logs/`, `config/`, and `plugins/` into containers.

## Project-Specific Patterns

### Data Validation
- **Always use Pydantic v2** with `ConfigDict`, `Field`, and `@field_validator` (see `observation.py`, `historical.py`)
- Field validators use `mode="after"` for cross-field validation; use `ValidationInfo` for field name context
- Set `extra="forbid"` in `ConfigDict` to reject unknown fields
- Include `json_schema_extra` examples in schema definitions for documentation

### DataFrame Conversions
- **Pydantic → DataFrame**: Use `pd.DataFrame([rec.model_dump() for rec in records])` (see `baseline.py:9`, `merge.py:7`)
- **Date Handling**: 
  - Historical records use `date` type; observations use `datetime`
  - Convert to string via `.dt.strftime("%m-%d")` for merging by calendar day
  - Avoid time zones; normalize to date objects for joins

### Statistical Operations
- Baseline stats computed as aggregations by calendar day (month-day)
- Quantiles: Use `.quantile(0.10)` and `.quantile(0.90)` for q10/q90 (not percentile)
- Anomaly enrichment: Always compute as `(value - mean) / std` for z-scores; handle NaN from missing baseline gracefully

### HTTP Requests & Retries
- Import from `requests.exceptions import HTTPError`
- Check `exc.response.status_code` directly (not exception properties)
- Retry loop pattern: `for n in range(RETRIES):` with `time.sleep(2 ** (n + 1))` for exponential backoff
- Return data only on success (`.json().get("results", [])` for NOAA; full response for NWS)

## Key Files Reference
| File | Purpose |
|------|---------|
| `src/schemas/observation.py` | Real-time observation schema (temperature, wind) |
| `src/schemas/historical.py` | Historical daily record schema (datatype-validated) |
| `src/fetch/noaa.py` | NOAA API fetcher with retry + token auth |
| `src/fetch/nws.py` | NWS API fetcher (WIP: parsing not yet implemented) |
| `src/transform/baseline.py` | Baseline stats computation & anomaly enrichment |
| `src/transform/merge.py` | Observation-historical merge by station + month-day |
| `dags/computations.py` | Airflow DAG definition |
| `tests/` | pytest suite for schemas, baseline, merge logic |
| `docker-compose.yaml` | Airflow orchestration stack (PostgreSQL, Redis, CeleryExecutor) |

## Common Gotchas
1. **Credential Loading**: `cred.json` token loaded at import; ensure file exists before importing `fetch.noaa`
2. **Station IDs**: NOAA uses GHCND codes (e.g., `USW00094847`); NWS uses WFO codes (e.g., `KDTW`)
3. **Datatype Validation**: Only TMAX, TMIN, TAVG, PRCP, SNOW, SNWD are allowed; adding new types requires schema update
4. **Negative Values**: Temperature/wind must be ≥ 0; precipitation must be ≥ 0 in validator logic
5. **Timezone Handling**: All timestamps assumed UTC; no TZ conversion implemented
