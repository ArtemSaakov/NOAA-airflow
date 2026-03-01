"""Pydantic schema for NOAA GHCND historical daily records.

This module defines the canonical structure for historical daily summaries
used throughout the pipeline. It enforces required fields, forbids unknown
keys, and constrains `datatype` to the supported GHCND codes.

**Key Behaviors:**
- Rejects extra fields via `extra="forbid"`
- Validates `datatype` against: TMAX, TMIN, TAVG, PRCP, SNOW, SNWD
- Leaves value ranges invalidated to allow negative temperatures

**Usage:**
- Models are used after fetch-layer conversions (e.g., TAVG in °C)
- See `src/fetch/noaa.py` for unit conversion details
"""

from typing import Optional
from datetime import date
from pydantic import BaseModel, ConfigDict, Field, field_validator


class HistoricalDailyRecord(BaseModel):
    """Schema for a single daily historical weather record from NOAA GHCND.

    Uses Pydantic v2 to enforce strict field definitions, validate datatypes,
    and generate JSON schema metadata for documentation and testing.
    """

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "station_id": "USW00094847",
                "record_date": "2025-10-17",
                "datatype": "TMAX",
                "value": 22.4,
                "attributes": "",
            }
        },
    )

    # Station identifier (NOAA GHCND code) for joining/lookup
    station_id: str = Field(..., description="Station identifier (NOAA GHCND code)")
    # Date of the daily summary (no time component)
    record_date: date = Field(..., description="Date of the daily summary (YYYY-MM-DD)")
    # Measurement type code constrained to known NOAA daily datatypes
    datatype: str = Field(
        ..., description="Measurement type code (e.g., TMAX, TMIN, PRCP)"
    )
    # Numeric measurement value; units depend on datatype
    value: float = Field(
        ...,
        description="Measured value. Units depend on `datatype` (e.g., TMAX/TMIN/TAVG in °C after any fetch-layer conversions). See fetch.process_historical for conversion notes.",
    )
    # Optional metadata string for flags/attributes
    attributes: Optional[str] = Field(
        None, description="Metadata or flags associated with the record"
    )

    # Validate that only supported datatype codes are accepted
    @field_validator("datatype", mode="after")
    @classmethod
    def confirm_known_datatype(cls, v: str) -> str:
        # Allowed Global Historical Climatology Network daily summary datatypes for this project
        allowed_types = {"TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD"}
        # Raise an error if an unexpected datatype appears
        if v not in allowed_types:
            raise ValueError(f"datatype must be one of {allowed_types}, got {v}")
        # Return validated datatype
        return v

    # Validate that precipitation and snow values are non-negative
    @field_validator("value", mode="after")
    @classmethod
    def validate_non_negative_precip(cls, v: float, info) -> float:
        # Get the datatype from validation context
        datatype = info.data.get("datatype")
        # Precipitation, snow, and snow depth cannot be negative
        non_negative_types = {"PRCP", "SNOW", "SNWD"}
        if datatype in non_negative_types and v < 0:
            raise ValueError(f"{datatype} value cannot be negative, got {v}")
        return v
