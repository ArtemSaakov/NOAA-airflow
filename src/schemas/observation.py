from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field

class ObservationRecord(BaseModel):
    station_id: str = Field(..., description="Station identifier (NWS code)")
    timestamp: datetime = Field(..., description="Timestamp of the observation in UTC")
    temperature_c: Optional[float] = Field(None, description="Air temperature in °C")
    wind_speed_m_s: Optional[float] = Field(None, description="Wind speed in m/s")
    # humidity_pct: Optional[float] = Field(None, description="Relative humidity (%)")

    class Config:
        # allow population by field name or alias
        allow_population_by_field_name = True
        # JSON schema generation...
        schema_extra = {
            "example": {
                "station_id": "KDTW",
                "timestamp": "2025-10-18T12:34:56Z",
                "temperature_c": 15.3,
                "wind_speed_m_s": 4.2
            }
        }