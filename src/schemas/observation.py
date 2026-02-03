"""Pydantic schema for National Weather Service (NWS) real-time observation records.

This module defines the canonical structure for live observations ingested
from the NWS API. It enforces required fields, forbids unknown keys, and
validates that numeric measurements are non-negative when provided.

**Key Behaviors:**
- Rejects extra fields via `extra="forbid"`
- Ensures `temperature_c` and `wind_speed_m_s` are >= 0 when present
- Emits JSON schema metadata for documentation and testing

**Usage:**
- Records are parsed after fetch-layer normalization to UTC timestamps
"""

from typing import Optional
from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, field_validator, ValidationInfo


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
                "wind_speed_m_s": 4.2,
            }
        },
    )

    station_id: str = Field(..., description="Station identifier (NWS code)")
    timestamp: datetime = Field(..., description="Timestamp of the observation in UTC")
    temperature_c: Optional[float] = Field(None, description="Air temperature in °C")
    wind_speed_m_s: Optional[float] = Field(None, description="Wind speed in m/s")
    # humidity_pct: Optional[float] = Field(None, description="Relative humidity (%)")

    @field_validator("temperature_c", "wind_speed_m_s", mode="before")
    @classmethod
    def confirm_non_negative(cls, v: float, info: ValidationInfo):
        if v is not None and v < 0:
            raise ValueError(f"{info.field_name} must be non-negative")
        return v
