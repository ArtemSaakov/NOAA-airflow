from typing import Optional
from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, field_validator


class ObservationRecord(BaseModel):
    """Schema for a single weather observation record from NWS. Uses Pydantic_V2"""
    model_config = ConfigDict(
        extra="forbid",
        # JSON schema generation...
        json_schema_extra={
            "example": {
                "station_id": "KDTW",
                "timestamp": "2025-10-18T12:34:56Z",
                "temperature_c": 15.3,
                "wind_speed_m_s": 4.2
            }
        }
    )

    station_id: str = Field(..., description="Station identifier (NWS code)")
    timestamp: datetime = Field(...,
                                description="Timestamp of the observation in UTC")
    temperature_c: Optional[float] = Field(
        None, description="Air temperature in °C")
    wind_speed_m_s: Optional[float] = Field(
        None, description="Wind speed in m/s")
    # humidity_pct: Optional[float] = Field(None, description="Relative humidity (%)")

    @field_validator("temperature_c", "wind_speed_m_s", mode='before')
    @classmethod
    def confirm_non_negative(cls, v, info):
        if v is not None and v < 0:
            raise ValueError(f"{info.field_name} must be non-negative")
        return v
