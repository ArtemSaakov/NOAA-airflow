from typing import Optional
from datetime import date
from pydantic import BaseModel, Field

class HistoricalDailyRecord(BaseModel):
    station_id: str = Field(..., description="Station identifier (NOAA GHCND code)")
    record_date: date = Field(..., description="Date of the daily summary (YYYY-MM-DD)")
    datatype: str = Field(..., description="Measurement type code (e.g., TMAX, TMIN, PRCP)")
    value: float = Field(..., description="Measured value (units indicated in context)")
    attributes: Optional[str] = Field(None, description="Metadata or flags associated with the record")

    class Config:
        allow_population_by_field_name = True
        schema_extra = {
            "example": {
                "station_id": "USW00094847",
                "record_date": "2025-10-17",
                "datatype": "TMAX",
                "value": 22.4,
                "attributes": ""
            }
        }
