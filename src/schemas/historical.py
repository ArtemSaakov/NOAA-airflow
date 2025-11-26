from typing import Optional
from datetime import date
from pydantic import BaseModel, ConfigDict, Field, field_validator, ValidationInfo


class HistoricalDailyRecord(BaseModel):
    """Schema for a single daily historical weather record from NOAA GHCND. Uses Pydantic v2."""
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "station_id": "USW00094847",
                "record_date": "2025-10-17",
                "datatype": "TMAX",
                "value": 22.4,
                "attributes": ""
            }
        }
    )

    station_id: str = Field(...,
                            description="Station identifier (NOAA GHCND code)")
    record_date: date = Field(...,
                              description="Date of the daily summary (YYYY-MM-DD)")
    datatype: str = Field(...,
                          description="Measurement type code (e.g., TMAX, TMIN, PRCP)")
    value: float = Field(...,
                         description="Measured value. Units depend on `datatype` (e.g., TMAX/TMIN/TAVG in °C after any fetch-layer conversions). See fetch.process_historical for conversion notes.")
    attributes: Optional[str] = Field(
        None, description="Metadata or flags associated with the record")

    @field_validator("datatype", mode="after")
    @classmethod
    def confirm_known_datatype(cls, v: str) -> str:
        allowed_types = {"TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD"}
        if v not in allowed_types:
            raise ValueError(
                f"datatype must be one of {allowed_types}, got {v}")
        return v

    @field_validator("value", mode="after")
    @classmethod
    def confirm_non_negative(cls, v: float, info: ValidationInfo) -> float:
        if v < 0:
            raise ValueError(
                f"{info.field_name} must be non-negative, got {v}")
        return v
